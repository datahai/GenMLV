/*! @azure/msal-browser v5.15.0 2026-06-23 */
'use strict';
import { ProtocolUtils, createClientAuthError, ClientAuthErrorCodes } from '@azure/msal-common/browser';

/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */
/**
 * Extracts the BrowserStateObject from the state string.
 * @param browserCrypto
 * @param state
 */
function extractBrowserRequestState(browserCrypto, state, correlationId) {
    if (!state) {
        return null;
    }
    try {
        const requestStateObj = ProtocolUtils.parseRequestState(browserCrypto.base64Decode, state, correlationId);
        return requestStateObj.libraryState.meta;
    }
    catch (e) {
        throw createClientAuthError(ClientAuthErrorCodes.invalidState, correlationId);
    }
}

export { extractBrowserRequestState };
//# sourceMappingURL=BrowserProtocolUtils.mjs.map
