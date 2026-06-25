/*! @azure/msal-browser v5.15.0 2026-06-23 */
'use strict';
import { AuthError } from '@azure/msal-common/browser';
import { unsupportedMethod } from './NativeAuthErrorCodes.mjs';

/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */
class NestedAppAuthError extends AuthError {
    constructor(errorCode, correlationId, errorMessage) {
        super(errorCode, correlationId, errorMessage);
        Object.setPrototypeOf(this, NestedAppAuthError.prototype);
        this.name = "NestedAppAuthError";
    }
    static createUnsupportedError(correlationId) {
        /*
         * Some controller stubs (perf callbacks, getPerformanceClient, getRedirectResponse)
         * have no request param and thus no correlationId to forward
         */
        return new NestedAppAuthError(unsupportedMethod, correlationId || "");
    }
}

export { NestedAppAuthError };
//# sourceMappingURL=NestedAppAuthError.mjs.map
