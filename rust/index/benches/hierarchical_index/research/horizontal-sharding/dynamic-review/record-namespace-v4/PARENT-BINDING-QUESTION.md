# Separate construction provenance from the current publication

An immutable namespace artifact cannot contain the semantic hash of the complete deployment that contains that same artifact. Such a hash would depend on itself. Its construction receipt can bind the preceding deployment and preceding namespace root, while the current registered bundle binds the new namespace artifact and all saved record/vector roots.

The component currently calls both bindings a parent. That ambiguity blocks actual-store execution. No current publication or collection authority is established by accepting a caller-provided hash.

The next interface must obtain one complete view from the captured registered descriptor. That view supplies the exact current collection, record segment roots, canonical vectors, ownership maps and namespace artifact. It also supplies the namespace's immutable construction receipt. The auditor checks the namespace against that receipt and checks its actual saved contents against the current complete view. The next materialization uses the current deployment identity from the same captured view, rather than replacing the namespace's construction parent identity with an unrelated caller value.

Placement may retain the exact namespace artifact because record/global identity does not change. Its new current deployment fence changes. The construction receipt remains unchanged and still proves which namespace prefix is preserved. Repeated append creates a new namespace artifact and construction receipt that refer to the exact prior namespace, while one complete bundle makes the record and vector outputs eligible together.

A construction-only component can instead enforce equality between its supplied expected parent and the root's construction parent. That is a smaller proof, but it must remain explicit that it cannot reopen a newly published namespace as the next current parent. No actual execution is admitted until review resolves the intended interface.
