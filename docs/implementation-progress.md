# Zenoh Node implementation ledger
Source: user-provided Zenoh 学习版节点框架计划 (2026-10-03).

Tasks: JSON validation/lifecycle; Executor/Timer; Node; examples/docs; verification/review.
Pre-flight: pubsub -> Executor (native Sample polling); Executor -> Node (owned resource registration); Node -> examples (subclass/context manager). Interfaces compatible.
Ruling: work in the current directory because every project file is untracked; a Git worktree would omit the source. No commits requested.
Ruling: use zenoh.handlers.RingChannel, the actual 1.10.1 export corresponding to the plan's zenoh.RingChannel.
Baseline: sandbox prevents native POSIX shared-memory initialization; requesting unsandboxed tests.

Task 1: complete — validation/idempotent close tests failed first, then 4/4 passed; original 3/3 pass outside sandbox.
Task 2: complete — Executor absent on initial tests; implementation passes 8/8 deterministic/thread tests.
Task 3: complete — Node absent on initial tests; native capacity/ordering/thread/cleanup tests pass. Regression: executor close cleared subscriber registry, causing subscriber close to raise; idempotent deregistration fixes the reproduced failure.
Task 4: complete — example constructor tests failed on missing subclasses; implementation passes. README documents interfaces, direct-mode distinction and lifecycle.
Task 5: in progress — full suite 26/26 passes including independent processes and SIGINT; final review pending.

Final review: independent read-only reviewer (gpt-6-astra). Critical: none. Important: Timer quotient overflow/rounding; Node-owned publisher thread restrictions. Minor: none.
Final: fixed Timer advancement — tiny positive periods, .1-second rounding, next regular deadline, below-clock-precision construction tests RED->GREEN.
Final: fixed Node publisher ownership — cross-thread publish/close rejection before native actions test RED->GREEN. Direct publishers remain compatible.
Final: added nearest-timer idle-wait and simultaneous body/cleanup exception identity coverage.
Review scope decisions: hard realtime and interruption of slow callbacks remain excluded as specified (slow callbacks delay scheduling); native cross-platform/discovery reliability is outside this implementation review (verification ran on this machine); failed native cleanup is attempted once to preserve idempotence (no retry on repeated close); direct-mode wrappers preserve existing threading behavior (Node-only thread guards).
Task 5: complete — final `.venv/bin/python -m unittest discover -s tests -v` outside sandbox: 32/32 PASS, including original 3 tests and independent example processes/SIGINT. `.venv/bin/python -m compileall -q src tests`: PASS. No new dependencies. Changes remain in the supplied working directory.
