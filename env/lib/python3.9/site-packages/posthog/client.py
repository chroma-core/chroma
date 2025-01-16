import atexit
import logging
import numbers
import os
import sys
from datetime import datetime, timedelta
from uuid import UUID

from dateutil.tz import tzutc
from six import string_types

from posthog.consumer import Consumer
from posthog.exception_capture import DEFAULT_DISTINCT_ID, ExceptionCapture
from posthog.exception_utils import exc_info_from_error, exceptions_from_error_tuple, handle_in_app
from posthog.feature_flags import InconclusiveMatchError, match_feature_flag_properties
from posthog.poller import Poller
from posthog.request import DEFAULT_HOST, APIError, batch_post, decide, determine_server_host, get
from posthog.utils import SizeLimitedDict, clean, guess_timezone, remove_trailing_slash
from posthog.version import VERSION

try:
    import queue
except ImportError:
    import Queue as queue


ID_TYPES = (numbers.Number, string_types, UUID)
MAX_DICT_SIZE = 50_000


class Client(object):
    """Create a new PostHog client."""

    log = logging.getLogger("posthog")

    def __init__(
        self,
        api_key=None,
        host=None,
        debug=False,
        max_queue_size=10000,
        send=True,
        on_error=None,
        flush_at=100,
        flush_interval=0.5,
        gzip=False,
        max_retries=3,
        sync_mode=False,
        timeout=15,
        thread=1,
        poll_interval=30,
        personal_api_key=None,
        project_api_key=None,
        disabled=False,
        disable_geoip=True,
        historical_migration=False,
        feature_flags_request_timeout_seconds=3,
        enable_exception_autocapture=False,
        exception_autocapture_integrations=None,
        project_root=None,
    ):
        self.queue = queue.Queue(max_queue_size)

        # api_key: This should be the Team API Key (token), public
        self.api_key = project_api_key or api_key

        require("api_key", self.api_key, string_types)

        self.on_error = on_error
        self.debug = debug
        self.send = send
        self.sync_mode = sync_mode
        # Used for session replay URL generation - we don't want the server host here.
        self.raw_host = host or DEFAULT_HOST
        self.host = determine_server_host(host)
        self.gzip = gzip
        self.timeout = timeout
        self.feature_flags = None
        self.feature_flags_by_key = None
        self.group_type_mapping = None
        self.cohorts = None
        self.poll_interval = poll_interval
        self.feature_flags_request_timeout_seconds = feature_flags_request_timeout_seconds
        self.poller = None
        self.distinct_ids_feature_flags_reported = SizeLimitedDict(MAX_DICT_SIZE, set)
        self.disabled = disabled
        self.disable_geoip = disable_geoip
        self.historical_migration = historical_migration
        self.enable_exception_autocapture = enable_exception_autocapture
        self.exception_autocapture_integrations = exception_autocapture_integrations
        self.exception_capture = None

        if project_root is None:
            try:
                project_root = os.getcwd()
            except Exception:
                project_root = None

        self.project_root = project_root

        # personal_api_key: This should be a generated Personal API Key, private
        self.personal_api_key = personal_api_key
        if debug:
            # Ensures that debug level messages are logged when debug mode is on.
            # Otherwise, defaults to WARNING level. See https://docs.python.org/3/howto/logging.html#what-happens-if-no-configuration-is-provided
            logging.basicConfig()
            self.log.setLevel(logging.DEBUG)
        else:
            self.log.setLevel(logging.WARNING)

        if self.enable_exception_autocapture:
            self.exception_capture = ExceptionCapture(self, integrations=self.exception_autocapture_integrations)

        if sync_mode:
            self.consumers = None
        else:
            # On program exit, allow the consumer thread to exit cleanly.
            # This prevents exceptions and a messy shutdown when the
            # interpreter is destroyed before the daemon thread finishes
            # execution. However, it is *not* the same as flushing the queue!
            # To guarantee all messages have been delivered, you'll still need
            # to call flush().
            if send:
                atexit.register(self.join)
            for n in range(thread):
                self.consumers = []
                consumer = Consumer(
                    self.queue,
                    self.api_key,
                    host=self.host,
                    on_error=on_error,
                    flush_at=flush_at,
                    flush_interval=flush_interval,
                    gzip=gzip,
                    retries=max_retries,
                    timeout=timeout,
                    historical_migration=historical_migration,
                )
                self.consumers.append(consumer)

                # if we've disabled sending, just don't start the consumer
                if send:
                    consumer.start()

    def identify(self, distinct_id=None, properties=None, context=None, timestamp=None, uuid=None, disable_geoip=None):
        properties = properties or {}
        context = context or {}
        require("distinct_id", distinct_id, ID_TYPES)
        require("properties", properties, dict)

        msg = {
            "timestamp": timestamp,
            "context": context,
            "distinct_id": distinct_id,
            "$set": properties,
            "event": "$identify",
            "uuid": uuid,
        }

        return self._enqueue(msg, disable_geoip)

    def get_feature_variants(
        self, distinct_id, groups=None, person_properties=None, group_properties=None, disable_geoip=None
    ):
        resp_data = self.get_decide(distinct_id, groups, person_properties, group_properties, disable_geoip)
        return resp_data["featureFlags"]

    def get_feature_payloads(
        self, distinct_id, groups=None, person_properties=None, group_properties=None, disable_geoip=None
    ):
        resp_data = self.get_decide(distinct_id, groups, person_properties, group_properties, disable_geoip)
        return resp_data["featureFlagPayloads"]

    def get_decide(self, distinct_id, groups=None, person_properties=None, group_properties=None, disable_geoip=None):
        require("distinct_id", distinct_id, ID_TYPES)

        if disable_geoip is None:
            disable_geoip = self.disable_geoip

        if groups:
            require("groups", groups, dict)
        else:
            groups = {}

        request_data = {
            "distinct_id": distinct_id,
            "groups": groups,
            "person_properties": person_properties,
            "group_properties": group_properties,
            "disable_geoip": disable_geoip,
        }
        resp_data = decide(self.api_key, self.host, timeout=self.feature_flags_request_timeout_seconds, **request_data)

        return resp_data

    def capture(
        self,
        distinct_id=None,
        event=None,
        properties=None,
        context=None,
        timestamp=None,
        uuid=None,
        groups=None,
        send_feature_flags=False,
        disable_geoip=None,
    ):
        properties = properties or {}
        context = context or {}
        require("distinct_id", distinct_id, ID_TYPES)
        require("properties", properties, dict)
        require("event", event, string_types)

        msg = {
            "properties": properties,
            "timestamp": timestamp,
            "context": context,
            "distinct_id": distinct_id,
            "event": event,
            "uuid": uuid,
        }

        if groups:
            require("groups", groups, dict)
            msg["properties"]["$groups"] = groups

        extra_properties = {}
        feature_variants = {}
        if send_feature_flags:
            try:
                feature_variants = self.get_feature_variants(distinct_id, groups, disable_geoip=disable_geoip)
            except Exception as e:
                self.log.exception(f"[FEATURE FLAGS] Unable to get feature variants: {e}")

        elif self.feature_flags:
            # Local evaluation is enabled, flags are loaded, so try and get all flags we can without going to the server
            feature_variants = self.get_all_flags(
                distinct_id, groups=(groups or {}), disable_geoip=disable_geoip, only_evaluate_locally=True
            )

        for feature, variant in feature_variants.items():
            extra_properties[f"$feature/{feature}"] = variant

        active_feature_flags = [key for (key, value) in feature_variants.items() if value is not False]
        if active_feature_flags:
            extra_properties["$active_feature_flags"] = active_feature_flags

        if extra_properties:
            msg["properties"] = {**extra_properties, **msg["properties"]}

        return self._enqueue(msg, disable_geoip)

    def set(self, distinct_id=None, properties=None, context=None, timestamp=None, uuid=None, disable_geoip=None):
        properties = properties or {}
        context = context or {}
        require("distinct_id", distinct_id, ID_TYPES)
        require("properties", properties, dict)

        msg = {
            "timestamp": timestamp,
            "context": context,
            "distinct_id": distinct_id,
            "$set": properties,
            "event": "$set",
            "uuid": uuid,
        }

        return self._enqueue(msg, disable_geoip)

    def set_once(self, distinct_id=None, properties=None, context=None, timestamp=None, uuid=None, disable_geoip=None):
        properties = properties or {}
        context = context or {}
        require("distinct_id", distinct_id, ID_TYPES)
        require("properties", properties, dict)

        msg = {
            "timestamp": timestamp,
            "context": context,
            "distinct_id": distinct_id,
            "$set_once": properties,
            "event": "$set_once",
            "uuid": uuid,
        }

        return self._enqueue(msg, disable_geoip)

    def group_identify(
        self,
        group_type=None,
        group_key=None,
        properties=None,
        context=None,
        timestamp=None,
        uuid=None,
        disable_geoip=None,
    ):
        properties = properties or {}
        context = context or {}
        require("group_type", group_type, ID_TYPES)
        require("group_key", group_key, ID_TYPES)
        require("properties", properties, dict)

        msg = {
            "event": "$groupidentify",
            "properties": {
                "$group_type": group_type,
                "$group_key": group_key,
                "$group_set": properties,
            },
            "distinct_id": "${}_{}".format(group_type, group_key),
            "timestamp": timestamp,
            "context": context,
            "uuid": uuid,
        }

        return self._enqueue(msg, disable_geoip)

    def alias(self, previous_id=None, distinct_id=None, context=None, timestamp=None, uuid=None, disable_geoip=None):
        context = context or {}

        require("previous_id", previous_id, ID_TYPES)
        require("distinct_id", distinct_id, ID_TYPES)

        msg = {
            "properties": {
                "distinct_id": previous_id,
                "alias": distinct_id,
            },
            "timestamp": timestamp,
            "context": context,
            "event": "$create_alias",
            "distinct_id": previous_id,
        }

        return self._enqueue(msg, disable_geoip)

    def page(
        self, distinct_id=None, url=None, properties=None, context=None, timestamp=None, uuid=None, disable_geoip=None
    ):
        properties = properties or {}
        context = context or {}

        require("distinct_id", distinct_id, ID_TYPES)
        require("properties", properties, dict)

        require("url", url, string_types)
        properties["$current_url"] = url

        msg = {
            "event": "$pageview",
            "properties": properties,
            "timestamp": timestamp,
            "context": context,
            "distinct_id": distinct_id,
            "uuid": uuid,
        }

        return self._enqueue(msg, disable_geoip)

    def capture_exception(
        self,
        exception=None,
        distinct_id=DEFAULT_DISTINCT_ID,
        properties=None,
        context=None,
        timestamp=None,
        uuid=None,
        groups=None,
    ):
        # this function shouldn't ever throw an error, so it logs exceptions instead of raising them.
        # this is important to ensure we don't unexpectedly re-raise exceptions in the user's code.
        try:
            properties = properties or {}
            require("distinct_id", distinct_id, ID_TYPES)
            require("properties", properties, dict)

            if exception is not None:
                exc_info = exc_info_from_error(exception)
            else:
                exc_info = sys.exc_info()

            if exc_info is None or exc_info == (None, None, None):
                self.log.warning("No exception information available")
                return

            # Format stack trace like sentry
            all_exceptions_with_trace = exceptions_from_error_tuple(exc_info)

            # Add in-app property to frames in the exceptions
            event = handle_in_app(
                {
                    "exception": {
                        "values": all_exceptions_with_trace,
                    },
                },
                project_root=self.project_root,
            )
            all_exceptions_with_trace_and_in_app = event["exception"]["values"]

            properties = {
                "$exception_type": all_exceptions_with_trace_and_in_app[0].get("type"),
                "$exception_message": all_exceptions_with_trace_and_in_app[0].get("value"),
                "$exception_list": all_exceptions_with_trace_and_in_app,
                "$exception_personURL": f"{remove_trailing_slash(self.raw_host)}/project/{self.api_key}/person/{distinct_id}",
                **properties,
            }

            return self.capture(distinct_id, "$exception", properties, context, timestamp, uuid, groups)
        except Exception as e:
            self.log.exception(f"Failed to capture exception: {e}")

    def _enqueue(self, msg, disable_geoip):
        """Push a new `msg` onto the queue, return `(success, msg)`"""

        if self.disabled:
            return False, "disabled"

        timestamp = msg["timestamp"]
        if timestamp is None:
            timestamp = datetime.utcnow().replace(tzinfo=tzutc())

        require("timestamp", timestamp, datetime)
        require("context", msg["context"], dict)

        # add common
        timestamp = guess_timezone(timestamp)
        msg["timestamp"] = timestamp.isoformat()

        # only send if "uuid" is truthy
        if "uuid" in msg:
            uuid = msg.pop("uuid")
            if uuid:
                msg["uuid"] = stringify_id(uuid)

        if not msg.get("properties"):
            msg["properties"] = {}
        msg["properties"]["$lib"] = "posthog-python"
        msg["properties"]["$lib_version"] = VERSION

        if disable_geoip is None:
            disable_geoip = self.disable_geoip

        if disable_geoip:
            msg["properties"]["$geoip_disable"] = True

        msg["distinct_id"] = stringify_id(msg.get("distinct_id", None))

        msg = clean(msg)
        self.log.debug("queueing: %s", msg)

        # if send is False, return msg as if it was successfully queued
        if not self.send:
            return True, msg

        if self.sync_mode:
            self.log.debug("enqueued with blocking %s.", msg["event"])
            batch_post(
                self.api_key,
                self.host,
                gzip=self.gzip,
                timeout=self.timeout,
                batch=[msg],
                historical_migration=self.historical_migration,
            )

            return True, msg

        try:
            self.queue.put(msg, block=False)
            self.log.debug("enqueued %s.", msg["event"])
            return True, msg
        except queue.Full:
            self.log.warning("analytics-python queue is full")
            return False, msg

    def flush(self):
        """Forces a flush from the internal queue to the server"""
        queue = self.queue
        size = queue.qsize()
        queue.join()
        # Note that this message may not be precise, because of threading.
        self.log.debug("successfully flushed about %s items.", size)

    def join(self):
        """Ends the consumer thread once the queue is empty.
        Blocks execution until finished
        """
        for consumer in self.consumers:
            consumer.pause()
            try:
                consumer.join()
            except RuntimeError:
                # consumer thread has not started
                pass

        if self.poller:
            self.poller.stop()

    def shutdown(self):
        """Flush all messages and cleanly shutdown the client"""
        self.flush()
        self.join()

        if self.exception_capture:
            self.exception_capture.close()

    def _load_feature_flags(self):
        try:
            response = get(
                self.personal_api_key,
                f"/api/feature_flag/local_evaluation/?token={self.api_key}&send_cohorts",
                self.host,
                timeout=10,
            )

            self.feature_flags = response["flags"] or []
            self.feature_flags_by_key = {
                flag["key"]: flag for flag in self.feature_flags if flag.get("key") is not None
            }
            self.group_type_mapping = response["group_type_mapping"] or {}
            self.cohorts = response["cohorts"] or {}

        except APIError as e:
            if e.status == 401:
                self.log.error(
                    "[FEATURE FLAGS] Error loading feature flags: To use feature flags, please set a valid personal_api_key. More information: https://posthog.com/docs/api/overview"
                )
                if self.debug:
                    raise APIError(
                        status=401,
                        message="You are using a write-only key with feature flags. "
                        "To use feature flags, please set a personal_api_key "
                        "More information: https://posthog.com/docs/api/overview",
                    )
            else:
                self.log.error(f"[FEATURE FLAGS] Error loading feature flags: {e}")
        except Exception as e:
            self.log.warning(
                "[FEATURE FLAGS] Fetching feature flags failed with following error. We will retry in %s seconds."
                % self.poll_interval
            )
            self.log.warning(e)

        self._last_feature_flag_poll = datetime.utcnow().replace(tzinfo=tzutc())

    def load_feature_flags(self):
        if not self.personal_api_key:
            self.log.warning("[FEATURE FLAGS] You have to specify a personal_api_key to use feature flags.")
            self.feature_flags = []
            return

        self._load_feature_flags()
        if not (self.poller and self.poller.is_alive()):
            self.poller = Poller(interval=timedelta(seconds=self.poll_interval), execute=self._load_feature_flags)
            self.poller.start()

    def _compute_flag_locally(
        self,
        feature_flag,
        distinct_id,
        *,
        groups={},
        person_properties={},
        group_properties={},
        warn_on_unknown_groups=True,
    ):
        if feature_flag.get("ensure_experience_continuity", False):
            raise InconclusiveMatchError("Flag has experience continuity enabled")

        if not feature_flag.get("active"):
            return False

        flag_filters = feature_flag.get("filters") or {}
        aggregation_group_type_index = flag_filters.get("aggregation_group_type_index")
        if aggregation_group_type_index is not None:
            group_name = self.group_type_mapping.get(str(aggregation_group_type_index))

            if not group_name:
                self.log.warning(
                    f"[FEATURE FLAGS] Unknown group type index {aggregation_group_type_index} for feature flag {feature_flag['key']}"
                )
                # failover to `/decide/`
                raise InconclusiveMatchError("Flag has unknown group type index")

            if group_name not in groups:
                # Group flags are never enabled in `groups` aren't passed in
                # don't failover to `/decide/`, since response will be the same
                if warn_on_unknown_groups:
                    self.log.warning(
                        f"[FEATURE FLAGS] Can't compute group feature flag: {feature_flag['key']} without group names passed in"
                    )
                else:
                    self.log.debug(
                        f"[FEATURE FLAGS] Can't compute group feature flag: {feature_flag['key']} without group names passed in"
                    )
                return False

            focused_group_properties = group_properties[group_name]
            return match_feature_flag_properties(feature_flag, groups[group_name], focused_group_properties)
        else:
            return match_feature_flag_properties(feature_flag, distinct_id, person_properties, self.cohorts)

    def feature_enabled(
        self,
        key,
        distinct_id,
        *,
        groups={},
        person_properties={},
        group_properties={},
        only_evaluate_locally=False,
        send_feature_flag_events=True,
        disable_geoip=None,
    ):
        response = self.get_feature_flag(
            key,
            distinct_id,
            groups=groups,
            person_properties=person_properties,
            group_properties=group_properties,
            only_evaluate_locally=only_evaluate_locally,
            send_feature_flag_events=send_feature_flag_events,
            disable_geoip=disable_geoip,
        )

        if response is None:
            return None
        return bool(response)

    def get_feature_flag(
        self,
        key,
        distinct_id,
        *,
        groups={},
        person_properties={},
        group_properties={},
        only_evaluate_locally=False,
        send_feature_flag_events=True,
        disable_geoip=None,
    ):
        require("key", key, string_types)
        require("distinct_id", distinct_id, ID_TYPES)
        require("groups", groups, dict)

        if self.disabled:
            return None

        person_properties, group_properties = self._add_local_person_and_group_properties(
            distinct_id, groups, person_properties, group_properties
        )

        if self.feature_flags is None and self.personal_api_key:
            self.load_feature_flags()
        response = None

        # If loading in previous line failed
        if self.feature_flags:
            for flag in self.feature_flags:
                if flag["key"] == key:
                    try:
                        response = self._compute_flag_locally(
                            flag,
                            distinct_id,
                            groups=groups,
                            person_properties=person_properties,
                            group_properties=group_properties,
                        )
                        self.log.debug(f"Successfully computed flag locally: {key} -> {response}")
                    except InconclusiveMatchError as e:
                        self.log.debug(f"Failed to compute flag {key} locally: {e}")
                        continue
                    except Exception as e:
                        self.log.exception(f"[FEATURE FLAGS] Error while computing variant locally: {e}")
                        continue

        flag_was_locally_evaluated = response is not None
        if not flag_was_locally_evaluated and not only_evaluate_locally:
            try:
                feature_flags = self.get_feature_variants(
                    distinct_id,
                    groups=groups,
                    person_properties=person_properties,
                    group_properties=group_properties,
                    disable_geoip=disable_geoip,
                )
                response = feature_flags.get(key)
                if response is None:
                    response = False
                self.log.debug(f"Successfully computed flag remotely: #{key} -> #{response}")
            except Exception as e:
                self.log.exception(f"[FEATURE FLAGS] Unable to get flag remotely: {e}")

        feature_flag_reported_key = f"{key}_{str(response)}"
        if (
            feature_flag_reported_key not in self.distinct_ids_feature_flags_reported[distinct_id]
            and send_feature_flag_events  # noqa: W503
        ):
            self.capture(
                distinct_id,
                "$feature_flag_called",
                {
                    "$feature_flag": key,
                    "$feature_flag_response": response,
                    "locally_evaluated": flag_was_locally_evaluated,
                    f"$feature/{key}": response,
                },
                groups=groups,
                disable_geoip=disable_geoip,
            )
            self.distinct_ids_feature_flags_reported[distinct_id].add(feature_flag_reported_key)
        return response

    def get_feature_flag_payload(
        self,
        key,
        distinct_id,
        *,
        match_value=None,
        groups={},
        person_properties={},
        group_properties={},
        only_evaluate_locally=False,
        send_feature_flag_events=True,
        disable_geoip=None,
    ):
        if self.disabled:
            return None

        if match_value is None:
            match_value = self.get_feature_flag(
                key,
                distinct_id,
                groups=groups,
                person_properties=person_properties,
                group_properties=group_properties,
                send_feature_flag_events=send_feature_flag_events,
                only_evaluate_locally=True,
                disable_geoip=disable_geoip,
            )

        response = None

        if match_value is not None:
            response = self._compute_payload_locally(key, match_value)

        if response is None and not only_evaluate_locally:
            decide_payloads = self.get_feature_payloads(
                distinct_id, groups, person_properties, group_properties, disable_geoip
            )
            response = decide_payloads.get(str(key).lower(), None)

        return response

    def _compute_payload_locally(self, key, match_value):
        payload = None

        if self.feature_flags_by_key is None:
            return payload

        flag_definition = self.feature_flags_by_key.get(key) or {}
        flag_filters = flag_definition.get("filters") or {}
        flag_payloads = flag_filters.get("payloads") or {}
        payload = flag_payloads.get(str(match_value).lower(), None)
        return payload

    def get_all_flags(
        self,
        distinct_id,
        *,
        groups={},
        person_properties={},
        group_properties={},
        only_evaluate_locally=False,
        disable_geoip=None,
    ):
        flags = self.get_all_flags_and_payloads(
            distinct_id,
            groups=groups,
            person_properties=person_properties,
            group_properties=group_properties,
            only_evaluate_locally=only_evaluate_locally,
            disable_geoip=disable_geoip,
        )
        return flags["featureFlags"]

    def get_all_flags_and_payloads(
        self,
        distinct_id,
        *,
        groups={},
        person_properties={},
        group_properties={},
        only_evaluate_locally=False,
        disable_geoip=None,
    ):
        if self.disabled:
            return {"featureFlags": None, "featureFlagPayloads": None}

        person_properties, group_properties = self._add_local_person_and_group_properties(
            distinct_id, groups, person_properties, group_properties
        )

        flags, payloads, fallback_to_decide = self._get_all_flags_and_payloads_locally(
            distinct_id, groups=groups, person_properties=person_properties, group_properties=group_properties
        )
        response = {"featureFlags": flags, "featureFlagPayloads": payloads}

        if fallback_to_decide and not only_evaluate_locally:
            try:
                flags_and_payloads = self.get_decide(
                    distinct_id,
                    groups=groups,
                    person_properties=person_properties,
                    group_properties=group_properties,
                    disable_geoip=disable_geoip,
                )
                response = flags_and_payloads
            except Exception as e:
                self.log.exception(f"[FEATURE FLAGS] Unable to get feature flags and payloads: {e}")

        return response

    def _get_all_flags_and_payloads_locally(
        self, distinct_id, *, groups={}, person_properties={}, group_properties={}, warn_on_unknown_groups=False
    ):
        require("distinct_id", distinct_id, ID_TYPES)
        require("groups", groups, dict)

        if self.feature_flags is None and self.personal_api_key:
            self.load_feature_flags()

        flags = {}
        payloads = {}
        fallback_to_decide = False
        # If loading in previous line failed
        if self.feature_flags:
            for flag in self.feature_flags:
                try:
                    flags[flag["key"]] = self._compute_flag_locally(
                        flag,
                        distinct_id,
                        groups=groups,
                        person_properties=person_properties,
                        group_properties=group_properties,
                        warn_on_unknown_groups=warn_on_unknown_groups,
                    )
                    matched_payload = self._compute_payload_locally(flag["key"], flags[flag["key"]])
                    if matched_payload:
                        payloads[flag["key"]] = matched_payload
                except InconclusiveMatchError:
                    # No need to log this, since it's just telling us to fall back to `/decide`
                    fallback_to_decide = True
                except Exception as e:
                    self.log.exception(f"[FEATURE FLAGS] Error while computing variant and payload: {e}")
                    fallback_to_decide = True
        else:
            fallback_to_decide = True

        return flags, payloads, fallback_to_decide

    def feature_flag_definitions(self):
        return self.feature_flags

    def _add_local_person_and_group_properties(self, distinct_id, groups, person_properties, group_properties):
        all_person_properties = {"distinct_id": distinct_id, **(person_properties or {})}

        all_group_properties = {}
        if groups:
            for group_name in groups:
                all_group_properties[group_name] = {
                    "$group_key": groups[group_name],
                    **(group_properties.get(group_name) or {}),
                }

        return all_person_properties, all_group_properties


def require(name, field, data_type):
    """Require that the named `field` has the right `data_type`"""
    if not isinstance(field, data_type):
        msg = "{0} must have {1}, got: {2}".format(name, data_type, field)
        raise AssertionError(msg)


def stringify_id(val):
    if val is None:
        return None
    if isinstance(val, string_types):
        return val
    return str(val)
