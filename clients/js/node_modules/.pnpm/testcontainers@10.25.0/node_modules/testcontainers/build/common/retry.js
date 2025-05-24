"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.IntervalRetry = void 0;
const clock_1 = require("./clock");
class AbstractRetry {
    clock;
    constructor(clock = new clock_1.SystemClock()) {
        this.clock = clock;
    }
    hasTimedOut(timeout, startTime) {
        return this.clock.getTime() - startTime > timeout;
    }
    wait(duration) {
        return new Promise((resolve) => setTimeout(resolve, duration));
    }
}
class IntervalRetry extends AbstractRetry {
    interval;
    constructor(interval) {
        super();
        this.interval = interval;
    }
    async retryUntil(fn, predicate, onTimeout, timeout) {
        const startTime = this.clock.getTime();
        let attemptNumber = 0;
        let result = await fn(attemptNumber++);
        while (!(await predicate(result))) {
            if (this.hasTimedOut(timeout, startTime)) {
                return onTimeout();
            }
            await this.wait(this.interval);
            result = await fn(attemptNumber++);
        }
        return result;
    }
}
exports.IntervalRetry = IntervalRetry;
//# sourceMappingURL=retry.js.map