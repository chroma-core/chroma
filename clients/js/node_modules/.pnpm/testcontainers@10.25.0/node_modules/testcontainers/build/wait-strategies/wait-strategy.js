"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AbstractWaitStrategy = void 0;
class AbstractWaitStrategy {
    startupTimeout = 60000;
    startupTimeoutSet = false;
    withStartupTimeout(startupTimeout) {
        this.startupTimeout = startupTimeout;
        this.startupTimeoutSet = true;
        return this;
    }
    isStartupTimeoutSet() {
        return this.startupTimeoutSet;
    }
    getStartupTimeout() {
        return this.startupTimeout;
    }
}
exports.AbstractWaitStrategy = AbstractWaitStrategy;
//# sourceMappingURL=wait-strategy.js.map