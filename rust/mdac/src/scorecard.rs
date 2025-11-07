//! This module implements a scorecard for tracking active requests.
//!
//! The principle behind a scorecard is that traffic gets tagged with string labels, and then said
//! string labels become the basis for tracking and reporting on the traffic.  A set of application
//! rules define wildcard patterns that match the labels and the limit for a given pattern.
//!
//! To make this concrete, imagine you were writing a web service that needs to support many users,
//! but each user is allowed one operation per HTTP route and method.  You could tag the traffic
//! with e.g. `USER=<user>`, `METHOD=<method>`, and `ROUTE=<route>`, and then define rules that
//! limit the traffic.  For example, if we wanted to install said limit, but give the admins extra,
//! it might look like this (not actual syntax):
//!
//! ```text
//! USER=admin, METHOD=*, ROUTE=/admin/* -> 10
//! USER=* METHOD=* ROUTE=* -> 1
//! ```
//!
//! This allows the admins to make 10 requests to any route, but everyone else would be limited to
//! one request per route.  The scorecard enforces this limit.
//!
//! As an optimization, this library assumes that a 128-bit hash function colliding is sufficiently
//! low probability that we are OK with two different requests that match different rules being
//! comingled.  This allows our hash table to have zero allocations, which improves performance.
//!
//! This scorecard diverges significantly from others in the literature.  In particular, if no rule
//! matches, there will be no stored state in the scorecard.  This enables higher performance by
//! only requiring one entry in the scorecard for each matched rule.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};

use siphasher::sip128::SipHasher24;

pub use crate::fnmatch::Pattern;

///////////////////////////////////////// ScorecardMetrics /////////////////////////////////////////

/// Metrics about what's happening inside a scorecard.
pub trait ScorecardMetrics: std::fmt::Debug + Send + Sync {
    /// A clicker that increments every time a new scorecard is created.
    fn new_scorecard(&self) {}
    /// A scorecard successfully tracked a request matching N rules.
    fn successful_track(&self, _n: usize) {}
    /// A scorecard failed to track a request matching N rules on the R'th rule.
    fn failed_track(&self, _n: usize, _r: usize) {}
    /// A successful untrack.
    fn successful_untrack(&self) {}
}

impl ScorecardMetrics for () {}

impl<T: ScorecardMetrics> ScorecardMetrics for Arc<T> {
    fn new_scorecard(&self) {
        self.as_ref().new_scorecard()
    }

    fn successful_track(&self, n: usize) {
        self.as_ref().successful_track(n)
    }

    fn failed_track(&self, n: usize, r: usize) {
        self.as_ref().failed_track(n, r)
    }

    fn successful_untrack(&self) {
        self.as_ref().successful_untrack()
    }
}

impl<T: ScorecardMetrics> ScorecardMetrics for &T {
    fn new_scorecard(&self) {
        (*self).new_scorecard()
    }

    fn successful_track(&self, n: usize) {
        (*self).successful_track(n)
    }

    fn failed_track(&self, n: usize, r: usize) {
        (*self).failed_track(n, r)
    }

    fn successful_untrack(&self) {
        (*self).successful_untrack()
    }
}

///////////////////////////////////////////// Scorecard ////////////////////////////////////////////

/// Scorecard is a traffic tracking and limiting system.
///
/// It is a sync/send data structure that is implemented to be highly concurrent.
#[derive(Debug)]
pub struct Scorecard<'a> {
    metrics: &'a (dyn ScorecardMetrics),
    stride: usize,
    rules: Mutex<Arc<RuleEvaluator>>,
    buckets: Vec<Bucket>,
}

impl<'a> Scorecard<'a> {
    /// Create a new scorecard from rules and an estimate of the number of threads.
    ///
    /// The rules are followed to the letter.
    ///
    /// The estimate of the number of threads is used to pre-allocate the buckets.  There will be
    /// O(t^2) buckets for t threads.  If there are t active threads, we logically know that there
    /// will be t active requests.  By the birthday paradox, there's a 50% chance that with sqrt(t)
    /// threads active that two threads will have the same "birthday" (i.e. the same bucket).
    /// Thus, instantiate t^2 buckets to give a 50% chance of there being one collision at any
    /// given time.
    ///
    /// This is a heuristic, but one contended mutex and T-1 uncontended mutexes is a good
    /// tradeoff for some memory allocation.
    pub fn new(
        metrics: &'a dyn ScorecardMetrics,
        rules: Vec<Rule>,
        estimate_thread_count: NonZeroUsize,
    ) -> Self {
        metrics.new_scorecard();
        let stride = estimate_thread_count.get() * estimate_thread_count.get();
        let rules = Mutex::new(Arc::new(RuleEvaluator::from(rules)));
        let mut buckets = Vec::with_capacity(stride);
        for _ in 0..stride {
            buckets.push(Bucket::default());
        }
        Self {
            metrics,
            stride,
            rules,
            buckets,
        }
    }

    /// Load scorecard rules from a file.  The file format is one rule per line.  The first N-1
    /// columns of each line (variadic) are the pattern for the rule, and the final column is a
    /// numeric limit imposed by the rule.  For example:
    ///
    /// ```text
    /// op:read who:admin 10
    /// op:read who:* 1
    /// ```
    ///
    /// This example scorecard gives admin 10 concurrent requests, and everyone else one concurrent
    /// request per identity.  Thus, in two rules, we apply a blanket rule that matches for every
    /// user individually and a rule that overrides for a specific user named admin.
    pub fn load_rules(path: &Path) -> Result<Vec<Rule>, std::io::Error> {
        let rules = std::fs::read_to_string(path)?;
        rules
            .split_terminator('\n')
            .map(|s| s.parse::<Rule>())
            .collect::<Result<Vec<_>, _>>()
    }

    /// Track a request.  This returns a ticket to be passed to untrack.  RAII is not used because
    /// of FFI boundary support.
    pub fn track(&self, tags: &[&str]) -> Option<ScorecardTicket> {
        let rules = self.get_rules();
        let pointers = rules.evaluate(tags);
        let mut keys: Vec<u128> = pointers.iter().map(|(_, x)| *x).collect();
        for (idx, (rule, key)) in pointers.iter().enumerate() {
            assert_eq!(*key, keys[idx]);
            let bucket = self.bucket_for_key(*key);
            if !bucket.track(*key, rule.limit) {
                keys.truncate(idx);
                self._untrack(keys);
                self.metrics.failed_track(pointers.len(), idx);
                return None;
            }
        }
        self.metrics.successful_track(pointers.len());
        Some(ScorecardTicket { keys })
    }

    /// Untrack a previously generated ticket.  It is an error to return a ticket that was
    /// generated by another scorecard.
    pub fn untrack(&self, mut ticket: ScorecardTicket) {
        self._untrack(std::mem::take(&mut ticket.keys));
        self.metrics.successful_untrack();
    }

    /// An internal untrack that doesn't hit the metrics.
    fn _untrack(&self, keys: Vec<u128>) {
        for key in keys {
            let bucket = self.bucket_for_key(key);
            bucket.untrack(key);
        }
    }

    /// Atomically acquire a snapshot of the rules.  Linearizable.
    fn get_rules(&self) -> Arc<RuleEvaluator> {
        // SAFETY(rescrv):  Mutex poisoning.
        let rules = self.rules.lock().unwrap();
        Arc::clone(&rules)
    }

    /// The hash-chosen bucket for a given key.
    fn bucket_for_key(&self, key: u128) -> &Bucket {
        &self.buckets[(key % self.stride as u128) as usize]
    }
}

/////////////////////////////////////////////// Rule ///////////////////////////////////////////////

/// A rule specifies a set of patterns and a limit.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Rule {
    pub patterns: Vec<Pattern>,
    pub limit: usize,
}

impl Rule {
    /// Create a new rule.  The rule matches a set of tags when
    /// `patterns.all(|p| tags.any(|t| p.fnmatch(t)))`.
    pub fn new(patterns: Vec<Pattern>, limit: usize) -> Rule {
        Self { patterns, limit }
    }

    /// Given a set of tags, return the hashes of the rules that match.
    ///
    /// This will, conceptually filter the powerset of the tags to just those elements of the power
    /// set that match the rule.
    pub fn matches_for(&self, tags: &[&str]) -> impl Iterator<Item = u128> {
        if self.patterns.is_empty() {
            return vec![].into_iter();
        }
        let mut matches = Vec::with_capacity(self.patterns.len());
        for pattern in self.patterns.iter() {
            let mut for_this_pattern = vec![];
            for (index, tag) in tags.iter().enumerate() {
                if pattern.fnmatch(tag) {
                    for_this_pattern.push(index);
                }
            }
            matches.push(for_this_pattern);
        }
        let mut hashes = vec![];
        let mut indices = vec![0; matches.len()];
        while indices[0] < matches[0].len() {
            let mut bail = false;
            let mut acc = 0u128;
            for i in 0..matches.len() {
                if matches[i].len() == indices[i] {
                    bail = true;
                    break;
                }
                acc ^= Self::hash(tags[matches[i][indices[i]]]);
            }
            if !bail {
                hashes.push(acc);
            }
            for (index, (i, m)) in std::iter::zip(indices.iter_mut(), matches.iter())
                .enumerate()
                .rev()
            {
                *i += 1;
                if *i >= m.len() && index > 0 {
                    *i = 0;
                } else {
                    break;
                }
            }
        }
        hashes.into_iter()
    }

    /// Predictably turn a string into a u128 by hashing it.
    fn hash(tag: &str) -> u128 {
        const MAGIC_CONSTANT_FOR_HASHING: [u8; 16] = [
            0x63, 0x68, 0x72, 0x6f, 0x6d, 0x61, 0x20, 0x62, 0x65, 0x6e, 0x63, 0x68, 0x6d, 0x61,
            0x72, 0x6b,
        ];
        let hasher = SipHasher24::new_with_key(&MAGIC_CONSTANT_FOR_HASHING);
        let h = hasher.hash(tag.as_bytes());
        h.as_u128()
    }
}

impl std::str::FromStr for Rule {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let pieces = s.split_whitespace().collect::<Vec<_>>();
        if pieces.len() < 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "rule must follow PATTERN LIMIT; neither specified",
            ));
        }
        let num_rules = pieces.len() - 1;
        let Ok(limit) = pieces[num_rules].parse::<usize>() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "rule must have numeric LIMIT",
            ));
        };
        let Some(patterns) = pieces
            .into_iter()
            .take(num_rules)
            .map(Pattern::new)
            .collect::<Option<Vec<_>>>()
        else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "pattern must be a valid glob",
            ));
        };
        Ok(Rule::new(patterns, limit))
    }
}

////////////////////////////////////////// ScorecardTicket /////////////////////////////////////////

/// A scorecard ticket is returned by the scorecard upon entry.
/// It is to be returned to the scorecard when done.
#[derive(Debug)]
pub struct ScorecardTicket {
    keys: Vec<u128>,
}

/////////////////////////////////////////// RuleEvaluator //////////////////////////////////////////

/// A rule evaluator takes a set of rules and choses all rules that match a set of tags.  It is
/// logically akin to filtering the powerset of all rules to take the first matching rule for each
/// member of the powerset.
#[derive(Debug)]
struct RuleEvaluator {
    rules: Vec<Rule>,
}

impl RuleEvaluator {
    /// Turn a set of tags into their numeric representation and look up the tags according to the
    /// rules.  This does not evaluate the rules according to the state of the scorecard.  It is an
    /// unsynchronized lookup function.
    fn evaluate(&self, tags: &[&str]) -> Vec<(&Rule, u128)> {
        let mut pointers = vec![];
        let mut seen = vec![];
        for rule in self.rules.iter() {
            for hash in rule.matches_for(tags) {
                if !seen.contains(&hash) {
                    pointers.push((rule, hash));
                    seen.push(hash);
                }
            }
        }
        pointers
    }
}

impl From<Vec<Rule>> for RuleEvaluator {
    fn from(rules: Vec<Rule>) -> Self {
        Self { rules }
    }
}

////////////////////////////////////////////// Bucket //////////////////////////////////////////////

/// A bucket represents a hashed fragment of the scorecard.  Done for concurrency.
#[derive(Debug, Default)]
struct Bucket {
    active: Mutex<HashMap<u128, usize>>,
}

impl Bucket {
    /// Track a key within a bucket iff it won't exceed the limit.
    fn track(&self, key: u128, limit: usize) -> bool {
        // SAFETY(rescrv):  Mutex poisoning.
        let mut active = self.active.lock().unwrap();
        match active.entry(key) {
            Entry::Occupied(mut entry) => {
                if *entry.get() < limit {
                    *entry.get_mut() += 1;
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(entry) => {
                if limit == 0 {
                    return false;
                }
                entry.insert(1);
                true
            }
        }
    }

    /// Untrack a key within a bucket.  Must have previously returned true from track.
    fn untrack(&self, key: u128) {
        // SAFETY(rescrv):  Mutex poisoning.
        let mut active = self.active.lock().unwrap();
        match active.entry(key) {
            Entry::Occupied(mut entry) => {
                let count = entry.get_mut();
                if *count == 1 {
                    entry.remove();
                } else {
                    *count -= 1;
                }
            }
            Entry::Vacant(_) => {
                // TODO(rescrv): counter
            }
        }
    }
}

////////////////////////////////////////// ScorecardGuard //////////////////////////////////////////

#[derive(Debug)]
pub struct ScorecardGuard {
    scorecard: Arc<Scorecard<'static>>,
    ticket: Option<ScorecardTicket>,
}

impl ScorecardGuard {
    pub fn new(scorecard: Arc<Scorecard<'static>>, ticket: Option<ScorecardTicket>) -> Self {
        Self { scorecard, ticket }
    }
}

impl Drop for ScorecardGuard {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.scorecard.untrack(ticket);
        }
    }
}

/////////////////////////////////////////////// tests //////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Debug, Default)]
    pub struct TestMetrics {
        new_scorecard: AtomicUsize,
        successful_track: AtomicUsize,
        failed_track: AtomicUsize,
        successful_untrack: AtomicUsize,
    }

    impl ScorecardMetrics for TestMetrics {
        fn new_scorecard(&self) {
            self.new_scorecard.fetch_add(1, Ordering::SeqCst);
        }

        fn successful_track(&self, _n: usize) {
            self.successful_track.fetch_add(1, Ordering::SeqCst);
        }

        fn failed_track(&self, _n: usize, _r: usize) {
            self.failed_track.fetch_add(1, Ordering::SeqCst);
        }

        fn successful_untrack(&self) {
            self.successful_untrack.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn empty() {
        let metrics = TestMetrics::default();
        let sc = Scorecard::new(&metrics, vec![], 1.try_into().unwrap());
        let ticket = sc.track(&["foo"]);
        assert!(ticket.is_some());
        sc.untrack(ticket.unwrap());
        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn simple_rule() {
        let metrics = TestMetrics::default();
        let sc = Scorecard::new(
            &metrics,
            vec![Rule {
                patterns: vec![Pattern::must("op:*".to_string())],
                limit: 2,
            }],
            1.try_into().unwrap(),
        );
        let ticket1 = sc.track(&["op:foo"]);
        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 0);
        let ticket2 = sc.track(&["op:foo"]);
        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 2);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 0);
        let ticket3 = sc.track(&["op:foo"]);
        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 2);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 1);
        let ticket4 = sc.track(&["op:bar"]);
        assert!(ticket1.is_some());
        assert!(ticket2.is_some());
        assert!(ticket3.is_none());
        assert!(ticket4.is_some());
        sc.untrack(ticket2.unwrap());
        let ticket5 = sc.track(&["op:foo"]);
        assert!(ticket5.is_some());
        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 4);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn basics() {
        let metrics = TestMetrics::default();
        // NOTE(rescrv):  We diverge from the upstream behavior in the Dropbox load management
        // library here.  If a tag doesn't match a rule, it won't be tracked by the scorecard for
        // efficiency reasons.  Thus, an empty scorecard admits everything.
        let req1 = &["meta_www", "TeamUserAssoc", "GID_10", "Point_Read"];
        let req2 = &["meta_api", "UserEntity", "GID_20", "Point_Read"];
        let req3 = &["meta_www", "TeamUserAssoc", "GID_30", "List_Read"];
        let sc = Scorecard::new(&metrics, vec![], 1.try_into().unwrap());

        let ret1 = sc.track(req1);
        assert!(ret1.is_some());

        let ret2 = sc.track(req2);
        assert!(ret2.is_some());

        let ret3 = sc.track(req3);
        assert!(ret3.is_some());

        sc.untrack(ret1.unwrap());
        sc.untrack(ret2.unwrap());
        sc.untrack(ret3.unwrap());

        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 3);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 3);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn wildcard() {
        let metrics = TestMetrics::default();
        let sc = Scorecard::new(
            &metrics,
            vec![Rule::new(
                vec![Pattern::must("op:*"), Pattern::must("client:*")],
                10,
            )],
            1.try_into().unwrap(),
        );
        let mut saved_tickets = vec![];
        // Fill to the limit
        for _ in 0..10 {
            let t = sc.track(&["op:read", "client:robert"]);
            assert!(t.is_some());
            saved_tickets.push(t);
        }
        // Reject
        let t = sc.track(&["op:read", "client:robert"]);
        assert!(t.is_none());
        // Fill in another dimension
        for _ in 0..10 {
            let t = sc.track(&["op:read", "client:alice"]);
            assert!(t.is_some());
            saved_tickets.push(t);
        }
        // Reject
        let t = sc.track(&["op:read", "client:alice"]);
        assert!(t.is_none());

        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 20);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn rule_precedence1() {
        // Test a specific override.
        let metrics = TestMetrics::default();
        let sc = Scorecard::new(
            &metrics,
            vec![
                Rule::new(
                    vec![Pattern::must("op:read"), Pattern::must("client:robert")],
                    20,
                ),
                Rule::new(vec![Pattern::must("op:*"), Pattern::must("client:*")], 10),
            ],
            1.try_into().unwrap(),
        );
        let mut saved_tickets = vec![];
        // Fill to the limit specified in the first rule.
        for _ in 0..20 {
            let t = sc.track(&["op:read", "client:robert"]);
            assert!(t.is_some());
            saved_tickets.push(t);
        }
        // Reject
        let t = sc.track(&["op:read", "client:robert"]);
        assert!(t.is_none());

        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 20);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn rule_precedence2() {
        // Test an exact pattern-based override.
        let metrics = TestMetrics::default();
        let sc = Scorecard::new(
            &metrics,
            vec![
                Rule::new(vec![Pattern::must("op:*"), Pattern::must("client:*")], 20),
                Rule::new(vec![Pattern::must("op:*"), Pattern::must("client:*")], 10),
            ],
            1.try_into().unwrap(),
        );
        let mut saved_tickets = vec![];
        // Fill to the limit specified in the first rule.
        for _ in 0..20 {
            let t = sc.track(&["op:read", "client:robert"]);
            assert!(t.is_some());
            saved_tickets.push(t);
        }
        // Reject
        let t = sc.track(&["op:read", "client:robert"]);
        assert!(t.is_none());

        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 20);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn three_patterns() {
        // Test an exact pattern-based override.
        let metrics = TestMetrics::default();
        let sc = Scorecard::new(
            &metrics,
            vec![
                Rule::new(
                    vec![
                        Pattern::must("op:*"),
                        Pattern::must("tenant:me"),
                        Pattern::must("collection:*"),
                    ],
                    20,
                ),
                Rule::new(
                    vec![
                        Pattern::must("op:*"),
                        Pattern::must("tenant:*"),
                        Pattern::must("collection:*"),
                    ],
                    10,
                ),
            ],
            1.try_into().unwrap(),
        );
        let mut saved_tickets = vec![];
        // Fill to the limit specified in the first rule.
        for _ in 0..20 {
            let t = sc.track(&["op:read", "tenant:me", "collection:foo"]);
            assert!(t.is_some());
            saved_tickets.push(t);
        }
        // Reject
        let t = sc.track(&["op:read", "tenant:me", "collection:foo"]);
        assert!(t.is_none());
        // Fill to the limit specified in the second rule.
        for _ in 0..10 {
            let t = sc.track(&["op:read", "tenant:you", "collection:foo"]);
            assert!(t.is_some());
            saved_tickets.push(t);
        }
        // Reject
        let t = sc.track(&["op:read", "tenant:you", "collection:foo"]);
        assert!(t.is_none());

        assert_eq!(metrics.new_scorecard.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.successful_track.load(Ordering::SeqCst), 30);
        assert_eq!(metrics.successful_untrack.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.failed_track.load(Ordering::SeqCst), 2);
    }
}
