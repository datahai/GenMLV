import { BaseAuthRequest, ClientAssertion } from "@azure/msal-common/node";
/**
 * Internal request type for the user_fic grant.
 * Used by UserFederatedIdentityCredentialClient after base request initialization.
 * @internal
 */
export type CommonUserFederatedIdentityCredentialRequest = Omit<BaseAuthRequest, "extraQueryParameters" | "extraParameters"> & {
    /** The federated identity credential (instance token from Leg 2) */
    assertion: string;
    /** An optional per-request client assertion override */
    clientAssertion?: ClientAssertion;
    /** Target user's Object ID — exactly one of userObjectId or username must be provided */
    userObjectId?: string;
    /** Target user's UPN — exactly one of userObjectId or username must be provided */
    username?: string;
};
//# sourceMappingURL=CommonUserFederatedIdentityCredentialRequest.d.ts.map