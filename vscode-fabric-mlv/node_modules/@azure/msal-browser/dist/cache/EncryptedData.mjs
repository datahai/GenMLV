/*! @azure/msal-browser v5.15.0 2026-06-23 */
'use strict';
/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */
function isEncrypted(data) {
    return (data.hasOwnProperty("id") &&
        data.hasOwnProperty("nonce") &&
        data.hasOwnProperty("data"));
}

export { isEncrypted };
//# sourceMappingURL=EncryptedData.mjs.map
