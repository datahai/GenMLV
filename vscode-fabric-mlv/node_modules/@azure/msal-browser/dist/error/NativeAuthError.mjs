/*! @azure/msal-browser v5.15.0 2026-06-23 */
'use strict';
import { AuthError, createInteractionRequiredAuthError, InteractionRequiredAuthErrorCodes, InteractionRequiredAuthError } from '@azure/msal-common/browser';
import { getDefaultErrorMessage, createBrowserAuthError } from './BrowserAuthError.mjs';
import { pageException, contentError } from './NativeAuthErrorCodes.mjs';
import { DISABLED, UI_NOT_ALLOWED, NO_NETWORK, USER_CANCEL, USER_INTERACTION_REQUIRED, ACCOUNT_UNAVAILABLE } from '../broker/nativeBroker/NativeStatusCodes.mjs';
import { noNetworkConnectivity, userCancelled } from './BrowserAuthErrorCodes.mjs';

/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */
const INVALID_METHOD_ERROR = -2147186943;
class NativeAuthError extends AuthError {
    constructor(errorCode, correlationId, description, ext) {
        super(errorCode, correlationId, description || getDefaultErrorMessage(errorCode));
        Object.setPrototypeOf(this, NativeAuthError.prototype);
        this.name = "NativeAuthError";
        this.ext = ext;
    }
}
/**
 * These errors should result in a fallback to the 'standard' browser based auth flow.
 */
function isFatalNativeAuthError(error) {
    if (error.ext &&
        error.ext.status &&
        error.ext.status === DISABLED) {
        return true;
    }
    if (error.ext &&
        error.ext.error &&
        error.ext.error === INVALID_METHOD_ERROR) {
        return true;
    }
    switch (error.errorCode) {
        case contentError:
        case pageException:
            return true;
        default:
            return false;
    }
}
/**
 * Create the appropriate error object based on the WAM status code.
 * @param code
 * @param description
 * @param ext
 * @returns
 */
function createNativeAuthError(code, correlationId, description, ext) {
    let error;
    if (ext && ext.status) {
        switch (ext.status) {
            case ACCOUNT_UNAVAILABLE:
                error = createInteractionRequiredAuthError(InteractionRequiredAuthErrorCodes.nativeAccountUnavailable, correlationId, getDefaultErrorMessage(code));
                break;
            case USER_INTERACTION_REQUIRED:
                error = new InteractionRequiredAuthError(code, correlationId, description);
                break;
            case USER_CANCEL:
                error = createBrowserAuthError(userCancelled, correlationId);
                break;
            case NO_NETWORK:
                error = createBrowserAuthError(noNetworkConnectivity, correlationId);
                break;
            case UI_NOT_ALLOWED:
                error = createInteractionRequiredAuthError(InteractionRequiredAuthErrorCodes.uiNotAllowed, correlationId);
                break;
            default:
                error = new NativeAuthError(code, correlationId, description, ext);
        }
        return error;
    }
    error = new NativeAuthError(code, correlationId, description, ext);
    return error;
}

export { NativeAuthError, createNativeAuthError, isFatalNativeAuthError };
//# sourceMappingURL=NativeAuthError.mjs.map
