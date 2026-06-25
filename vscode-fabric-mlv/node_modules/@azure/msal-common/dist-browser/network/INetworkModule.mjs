/*! @azure/msal-common v16.10.0 2026-06-23 */
'use strict';
import { createClientAuthError } from '../error/ClientAuthError.mjs';
import { methodNotImplemented } from '../error/ClientAuthErrorCodes.mjs';

/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */
const StubbedNetworkModule = {
    // Module-level singleton: no per-request correlationId available
    sendGetRequestAsync: () => {
        return Promise.reject(createClientAuthError(methodNotImplemented, ""));
    },
    sendPostRequestAsync: () => {
        return Promise.reject(createClientAuthError(methodNotImplemented, ""));
    },
};

export { StubbedNetworkModule };
//# sourceMappingURL=INetworkModule.mjs.map
