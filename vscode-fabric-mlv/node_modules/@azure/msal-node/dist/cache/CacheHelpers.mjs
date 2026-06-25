/*! @azure/msal-node v5.3.0 2026-06-23 */
'use strict';
import { Constants } from '@azure/msal-common/node';
import { createHash } from 'crypto';
import { CACHE } from '../utils/Constants.mjs';

/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */
/**
 * Computes a combined hash from additional cache key components.
 * Matches the cross-SDK algorithm: sort keys → concatenate key+value → SHA-256 → Base64URL (no padding).
 */
function computeAdditionalCacheKeyHash(components) {
    const sortedKeys = Object.keys(components).sort();
    const input = sortedKeys.map((k) => k + components[k]).join("");
    return createHash("sha256").update(input, "utf8").digest("base64url");
}
function generateCredentialKey(credential) {
    const familyId = (credential.credentialType === Constants.CredentialType.REFRESH_TOKEN &&
        credential.familyId) ||
        credential.clientId;
    const scheme = credential.tokenType &&
        credential.tokenType.toLowerCase() !==
            Constants.AuthenticationScheme.BEARER.toLowerCase()
        ? credential.tokenType.toLowerCase()
        : "";
    const credentialKey = [
        credential.homeAccountId,
        credential.environment,
        credential.credentialType,
        familyId,
        credential.realm || "",
        credential.target || "",
        scheme,
    ];
    // Compute and append a combined hash from additional cache key components (e.g., fmi_path)
    if (credential.additionalCacheKeyComponents &&
        Object.keys(credential.additionalCacheKeyComponents).length > 0) {
        credentialKey.push(computeAdditionalCacheKeyHash(credential.additionalCacheKeyComponents));
    }
    return credentialKey.join(CACHE.KEY_SEPARATOR).toLowerCase();
}
function generateAccountKey(account) {
    const homeTenantId = account.homeAccountId.split(".")[1];
    const accountKey = [
        account.homeAccountId,
        account.environment,
        homeTenantId || account.tenantId || "",
    ];
    return accountKey.join(CACHE.KEY_SEPARATOR).toLowerCase();
}

export { generateAccountKey, generateCredentialKey };
//# sourceMappingURL=CacheHelpers.mjs.map
