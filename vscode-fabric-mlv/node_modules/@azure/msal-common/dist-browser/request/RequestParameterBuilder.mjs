/*! @azure/msal-common v16.10.0 2026-06-23 */
'use strict';
import { OIDC_DEFAULT_SCOPES, ResponseMode, HeaderNames, CLIENT_INFO, ClaimsRequestKeys, PasswordGrantConstants, AuthenticationScheme, X_MS_LIB_CAPABILITY_VALUE } from '../utils/Constants.mjs';
import { CLIENT_ID, BROKER_CLIENT_ID, REDIRECT_URI, RESPONSE_TYPE, RESPONSE_MODE, NATIVE_BROKER, SCOPE, POST_LOGOUT_URI, ID_TOKEN_HINT, DOMAIN_HINT, LOGIN_HINT, SID, CLAIMS, CLIENT_REQUEST_ID, X_CLIENT_SKU, X_CLIENT_VER, X_CLIENT_OS, X_CLIENT_CPU, X_APP_NAME, X_APP_VER, PROMPT, STATE, NONCE, CODE_CHALLENGE, CODE_CHALLENGE_METHOD, CODE, DEVICE_CODE, REFRESH_TOKEN, CODE_VERIFIER, CLIENT_SECRET, CLIENT_ASSERTION, CLIENT_ASSERTION_TYPE, OBO_ASSERTION, REQUESTED_TOKEN_USE, GRANT_TYPE, CLI_DATA, INSTANCE_AWARE, TOKEN_TYPE, REQ_CNF, X_CLIENT_CURR_TELEM, X_CLIENT_LAST_TELEM, X_MS_LIB_CAPABILITY, LOGOUT_HINT, BROKER_REDIRECT_URI, EAR_JWK, EAR_JWE_CRYPTO, RESOURCE } from '../constants/AADServerParamKeys.mjs';
import { ScopeSet } from './ScopeSet.mjs';
import { createClientConfigurationError } from '../error/ClientConfigurationError.mjs';
import { pkceParamsMissing, invalidClaims } from '../error/ClientConfigurationErrorCodes.mjs';

/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */
function instrumentBrokerParams(parameters, correlationId, performanceClient) {
    if (!correlationId) {
        return;
    }
    const clientId = parameters.get(CLIENT_ID);
    if (clientId && parameters.has(BROKER_CLIENT_ID)) {
        performanceClient?.addFields({
            embeddedClientId: clientId,
            embeddedRedirectUri: parameters.get(REDIRECT_URI),
        }, correlationId);
    }
}
/**
 * Add the given response_type
 * @param parameters
 * @param responseType
 */
function addResponseType(parameters, responseType) {
    parameters.set(RESPONSE_TYPE, responseType);
}
/**
 * add response_mode. defaults to query.
 * @param responseMode
 */
function addResponseMode(parameters, responseMode) {
    parameters.set(RESPONSE_MODE, responseMode ? responseMode : ResponseMode.QUERY);
}
/**
 * Add flag to indicate STS should attempt to use WAM if available
 */
function addNativeBroker(parameters) {
    parameters.set(NATIVE_BROKER, "1");
}
/**
 * add scopes. set addOidcScopes to false to prevent default scopes in non-user scenarios
 * @param scopeSet
 * @param addOidcScopes
 */
function addScopes(parameters, scopes, correlationId, addOidcScopes = true, defaultScopes = OIDC_DEFAULT_SCOPES) {
    // Always add openid to the scopes when adding OIDC scopes
    if (addOidcScopes &&
        !defaultScopes.includes("openid") &&
        !scopes.includes("openid")) {
        defaultScopes.push("openid");
    }
    const requestScopes = addOidcScopes
        ? [...(scopes || []), ...defaultScopes]
        : scopes || [];
    const scopeSet = new ScopeSet(requestScopes, correlationId);
    parameters.set(SCOPE, scopeSet.printScopes());
}
/**
 * add clientId
 * @param clientId
 */
function addClientId(parameters, clientId) {
    parameters.set(CLIENT_ID, clientId);
}
/**
 * add redirect_uri
 * @param redirectUri
 */
function addRedirectUri(parameters, redirectUri) {
    parameters.set(REDIRECT_URI, redirectUri);
}
/**
 * add post logout redirectUri
 * @param redirectUri
 */
function addPostLogoutRedirectUri(parameters, redirectUri) {
    parameters.set(POST_LOGOUT_URI, redirectUri);
}
/**
 * add id_token_hint to logout request
 * @param idTokenHint
 */
function addIdTokenHint(parameters, idTokenHint) {
    parameters.set(ID_TOKEN_HINT, idTokenHint);
}
/**
 * add domain_hint
 * @param domainHint
 */
function addDomainHint(parameters, domainHint) {
    parameters.set(DOMAIN_HINT, domainHint);
}
/**
 * add login_hint
 * @param loginHint
 */
function addLoginHint(parameters, loginHint) {
    parameters.set(LOGIN_HINT, loginHint);
}
/**
 * Adds the CCS (Cache Credential Service) query parameter for login_hint
 * @param loginHint
 */
function addCcsUpn(parameters, loginHint) {
    parameters.set(HeaderNames.CCS_HEADER, `UPN:${loginHint}`);
}
/**
 * Adds the CCS (Cache Credential Service) query parameter for account object
 * @param loginHint
 */
function addCcsOid(parameters, clientInfo) {
    parameters.set(HeaderNames.CCS_HEADER, `Oid:${clientInfo.uid}@${clientInfo.utid}`);
}
/**
 * add sid
 * @param sid
 */
function addSid(parameters, sid) {
    parameters.set(SID, sid);
}
/**
 * Adds claims to request parameters, conditionally excluding clientCapabilities
 * when skipBrokerClaims is true and a brokered flow is in effect.
 * @param parameters - The request parameters map
 * @param correlationId - The request correlation id
 * @param claims - The claims string from the request
 * @param clientCapabilities - The client capabilities from configuration
 * @param skipBrokerClaims - When true and BROKER_CLIENT_ID is present, excludes clientCapabilities from claims
 */
function addClaims(parameters, correlationId, claims, clientCapabilities, skipBrokerClaims) {
    // Skip clientCapabilities if skipBrokerClaims is set to true and this is a brokered authentication flow
    const configClaims = skipBrokerClaims && parameters.has(BROKER_CLIENT_ID)
        ? undefined
        : clientCapabilities;
    const mergedClaims = buildMergedClaims(claims, configClaims, correlationId);
    parameters.set(CLAIMS, mergedClaims);
}
/**
 * add correlationId
 * @param correlationId
 */
function addCorrelationId(parameters, correlationId) {
    parameters.set(CLIENT_REQUEST_ID, correlationId);
}
/**
 * add library info query params
 * @param libraryInfo
 */
function addLibraryInfo(parameters, libraryInfo) {
    // Telemetry Info
    parameters.set(X_CLIENT_SKU, libraryInfo.sku);
    parameters.set(X_CLIENT_VER, libraryInfo.version);
    if (libraryInfo.os) {
        parameters.set(X_CLIENT_OS, libraryInfo.os);
    }
    if (libraryInfo.cpu) {
        parameters.set(X_CLIENT_CPU, libraryInfo.cpu);
    }
}
/**
 * Add client telemetry parameters
 * @param appTelemetry
 */
function addApplicationTelemetry(parameters, appTelemetry) {
    if (appTelemetry?.appName) {
        parameters.set(X_APP_NAME, appTelemetry.appName);
    }
    if (appTelemetry?.appVersion) {
        parameters.set(X_APP_VER, appTelemetry.appVersion);
    }
}
/**
 * add prompt
 * @param prompt
 */
function addPrompt(parameters, prompt) {
    parameters.set(PROMPT, prompt);
}
/**
 * add state
 * @param state
 */
function addState(parameters, state) {
    if (state) {
        parameters.set(STATE, state);
    }
}
/**
 * add nonce
 * @param nonce
 */
function addNonce(parameters, nonce) {
    parameters.set(NONCE, nonce);
}
/**
 * add code_challenge and code_challenge_method
 * - throw if either of them are not passed
 * @param codeChallenge
 * @param codeChallengeMethod
 */
function addCodeChallengeParams(parameters, codeChallenge, codeChallengeMethod) {
    if (codeChallenge && codeChallengeMethod) {
        parameters.set(CODE_CHALLENGE, codeChallenge);
        parameters.set(CODE_CHALLENGE_METHOD, codeChallengeMethod);
    }
    else {
        throw createClientConfigurationError(pkceParamsMissing, "");
    }
}
/**
 * add the `authorization_code` passed by the user to exchange for a token
 * @param code
 */
function addAuthorizationCode(parameters, code) {
    parameters.set(CODE, code);
}
/**
 * add the `authorization_code` passed by the user to exchange for a token
 * @param code
 */
function addDeviceCode(parameters, code) {
    parameters.set(DEVICE_CODE, code);
}
/**
 * add the `refreshToken` passed by the user
 * @param refreshToken
 */
function addRefreshToken(parameters, refreshToken) {
    parameters.set(REFRESH_TOKEN, refreshToken);
}
/**
 * add the `code_verifier` passed by the user to exchange for a token
 * @param codeVerifier
 */
function addCodeVerifier(parameters, codeVerifier) {
    parameters.set(CODE_VERIFIER, codeVerifier);
}
/**
 * add client_secret
 * @param clientSecret
 */
function addClientSecret(parameters, clientSecret) {
    parameters.set(CLIENT_SECRET, clientSecret);
}
/**
 * add clientAssertion for confidential client flows
 * @param clientAssertion
 */
function addClientAssertion(parameters, clientAssertion) {
    if (clientAssertion) {
        parameters.set(CLIENT_ASSERTION, clientAssertion);
    }
}
/**
 * add clientAssertionType for confidential client flows
 * @param clientAssertionType
 */
function addClientAssertionType(parameters, clientAssertionType) {
    if (clientAssertionType) {
        parameters.set(CLIENT_ASSERTION_TYPE, clientAssertionType);
    }
}
/**
 * add OBO assertion for confidential client flows
 * @param clientAssertion
 */
function addOboAssertion(parameters, oboAssertion) {
    parameters.set(OBO_ASSERTION, oboAssertion);
}
/**
 * add grant type
 * @param grantType
 */
function addRequestTokenUse(parameters, tokenUse) {
    parameters.set(REQUESTED_TOKEN_USE, tokenUse);
}
/**
 * add grant type
 * @param grantType
 */
function addGrantType(parameters, grantType) {
    parameters.set(GRANT_TYPE, grantType);
}
/**
 * add client info
 *
 */
function addClientInfo(parameters) {
    parameters.set(CLIENT_INFO, "1");
}
/**
 * add clidata=1 to request to indicate client data support
 */
function addCliData(parameters) {
    parameters.set(CLI_DATA, "1");
}
function addInstanceAware(parameters) {
    if (!parameters.has(INSTANCE_AWARE)) {
        parameters.set(INSTANCE_AWARE, "true");
    }
}
/**
 * Add extraParameters
 * @param extraParams - String dictionary containing extra parameters to be added.
 */
function addExtraParameters(parameters, extraParams) {
    Object.entries(extraParams).forEach(([key, value]) => {
        if (!parameters.has(key) && value) {
            parameters.set(key, value);
        }
    });
}
/**
 * Default optional idToken claims requested on all auth requests.
 * signin_state enables KMSI detection; login_hint enables login hint propagation.
 */
const DEFAULT_ID_TOKEN_CLAIMS = {
    [ClaimsRequestKeys.SIGNIN_STATE]: { essential: false },
    [ClaimsRequestKeys.LOGIN_HINT]: { essential: false },
};
/**
 * Parses claims JSON, merges default optional idToken claims (signin_state, login_hint),
 * and appends client capabilities (xms_cc) to the access_token section.
 * Does not overwrite idToken claims already specified by the caller.
 * @param claims - Existing claims JSON string from the request (may be undefined)
 * @param clientCapabilities - Client capabilities array from configuration
 * @returns Merged claims JSON string
 */
function buildMergedClaims(claims, clientCapabilities, correlationId = "") {
    let mergedClaims;
    // Parse provided claims into JSON object or initialize empty object
    if (!claims) {
        mergedClaims = {};
    }
    else {
        try {
            const parsed = JSON.parse(claims);
            if (typeof parsed !== "object" ||
                parsed === null ||
                Array.isArray(parsed)) {
                throw new Error("Claims must be a JSON object");
            }
            mergedClaims = parsed;
        }
        catch (e) {
            throw createClientConfigurationError(invalidClaims, correlationId);
        }
    }
    // Add default optional idToken claims
    if (!Object.prototype.hasOwnProperty.call(mergedClaims, ClaimsRequestKeys.ID_TOKEN)) {
        mergedClaims[ClaimsRequestKeys.ID_TOKEN] = {};
    }
    const idTokenClaims = mergedClaims[ClaimsRequestKeys.ID_TOKEN];
    for (const [key, value] of Object.entries(DEFAULT_ID_TOKEN_CLAIMS)) {
        if (!(key in idTokenClaims)) {
            idTokenClaims[key] = value;
        }
    }
    // Add client capabilities
    if (clientCapabilities && clientCapabilities.length > 0) {
        if (!Object.prototype.hasOwnProperty.call(mergedClaims, ClaimsRequestKeys.ACCESS_TOKEN)) {
            // Add access_token key to claims object
            mergedClaims[ClaimsRequestKeys.ACCESS_TOKEN] = {};
        }
        // Add xms_cc claim with provided clientCapabilities to access_token key
        mergedClaims[ClaimsRequestKeys.ACCESS_TOKEN][ClaimsRequestKeys.XMS_CC] = {
            values: clientCapabilities,
        };
    }
    return JSON.stringify(mergedClaims);
}
/**
 * adds `username` for Password Grant flow
 * @param username
 */
function addUsername(parameters, username) {
    parameters.set(PasswordGrantConstants.username, username);
}
/**
 * adds `password` for Password Grant flow
 * @param password
 */
function addPassword(parameters, password) {
    parameters.set(PasswordGrantConstants.password, password);
}
/**
 * add pop_jwk to query params
 * @param cnfString
 */
function addPopToken(parameters, cnfString) {
    if (cnfString) {
        parameters.set(TOKEN_TYPE, AuthenticationScheme.POP);
        parameters.set(REQ_CNF, cnfString);
    }
}
/**
 * add SSH JWK and key ID to query params
 */
function addSshJwk(parameters, sshJwkString) {
    if (sshJwkString) {
        parameters.set(TOKEN_TYPE, AuthenticationScheme.SSH);
        parameters.set(REQ_CNF, sshJwkString);
    }
}
/**
 * add server telemetry fields
 * @param serverTelemetryManager
 * @internal
 */
function addServerTelemetry(parameters, serverTelemetryManager) {
    parameters.set(X_CLIENT_CURR_TELEM, serverTelemetryManager.generateCurrentRequestHeaderValue());
    parameters.set(X_CLIENT_LAST_TELEM, serverTelemetryManager.generateLastRequestHeaderValue());
}
/**
 * Adds parameter that indicates to the server that throttling is supported
 */
function addThrottling(parameters) {
    parameters.set(X_MS_LIB_CAPABILITY, X_MS_LIB_CAPABILITY_VALUE);
}
/**
 * Adds logout_hint parameter for "silent" logout which prevent server account picker
 */
function addLogoutHint(parameters, logoutHint) {
    parameters.set(LOGOUT_HINT, logoutHint);
}
function addBrokerParameters(parameters, brokerClientId, brokerRedirectUri) {
    if (!parameters.has(BROKER_CLIENT_ID)) {
        parameters.set(BROKER_CLIENT_ID, brokerClientId);
    }
    if (!parameters.has(BROKER_REDIRECT_URI)) {
        parameters.set(BROKER_REDIRECT_URI, brokerRedirectUri);
    }
}
/**
 * Add EAR (Encrypted Authorize Response) request parameters
 * @param parameters
 * @param jwk
 */
function addEARParameters(parameters, jwk) {
    parameters.set(EAR_JWK, encodeURIComponent(jwk));
    // ear_jwe_crypto will always have value: {"alg":"dir","enc":"A256GCM"} so we can hardcode this
    const jweCryptoB64Encoded = "eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0";
    parameters.set(EAR_JWE_CRYPTO, jweCryptoB64Encoded);
}
function addResource(parameters, resource) {
    if (resource) {
        parameters.set(RESOURCE, resource);
    }
}

export { addApplicationTelemetry, addAuthorizationCode, addBrokerParameters, addCcsOid, addCcsUpn, addClaims, addCliData, addClientAssertion, addClientAssertionType, addClientId, addClientInfo, addClientSecret, addCodeChallengeParams, addCodeVerifier, addCorrelationId, addDeviceCode, addDomainHint, addEARParameters, addExtraParameters, addGrantType, addIdTokenHint, addInstanceAware, addLibraryInfo, addLoginHint, addLogoutHint, addNativeBroker, addNonce, addOboAssertion, addPassword, addPopToken, addPostLogoutRedirectUri, addPrompt, addRedirectUri, addRefreshToken, addRequestTokenUse, addResource, addResponseMode, addResponseType, addScopes, addServerTelemetry, addSid, addSshJwk, addState, addThrottling, addUsername, buildMergedClaims, instrumentBrokerParams };
//# sourceMappingURL=RequestParameterBuilder.mjs.map
