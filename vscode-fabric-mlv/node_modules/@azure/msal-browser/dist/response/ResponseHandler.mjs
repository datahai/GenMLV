/*! @azure/msal-browser v5.15.0 2026-06-23 */
'use strict';
import { UrlUtils } from '@azure/msal-common/browser';
import { createBrowserAuthError } from '../error/BrowserAuthError.mjs';
import { extractBrowserRequestState } from '../utils/BrowserProtocolUtils.mjs';
import { hashEmptyError, hashDoesNotContainKnownProperties, noStateInHash, unableToParseState, stateInteractionTypeMismatch } from '../error/BrowserAuthErrorCodes.mjs';

/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */
function deserializeResponse(responseString, responseLocation, logger, correlationId) {
    // Deserialize hash fragment response parameters.
    const serverParams = UrlUtils.getDeserializedResponse(responseString);
    if (!serverParams) {
        if (!UrlUtils.stripLeadingHashOrQuery(responseString)) {
            // Hash or Query string is empty
            logger.error(`18h0l1 ${responseLocation} ${responseLocation}`, correlationId);
            throw createBrowserAuthError(hashEmptyError, correlationId);
        }
        else {
            logger.error(`13pl0s ${responseLocation} ${responseLocation}`, correlationId);
            logger.errorPii(`1097vx ${responseLocation} ${responseString}`, correlationId);
            throw createBrowserAuthError(hashDoesNotContainKnownProperties, correlationId);
        }
    }
    return serverParams;
}
/**
 * Returns the interaction type that the response object belongs to
 */
function validateInteractionType(response, browserCrypto, interactionType, correlationId) {
    if (!response.state) {
        throw createBrowserAuthError(noStateInHash, correlationId);
    }
    const platformStateObj = extractBrowserRequestState(browserCrypto, response.state, correlationId);
    if (!platformStateObj) {
        throw createBrowserAuthError(unableToParseState, correlationId);
    }
    if (platformStateObj.interactionType !== interactionType) {
        throw createBrowserAuthError(stateInteractionTypeMismatch, correlationId);
    }
}

export { deserializeResponse, validateInteractionType };
//# sourceMappingURL=ResponseHandler.mjs.map
