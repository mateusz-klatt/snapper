# Backend-owned Android push

Android push is optional and disabled by default. Existing APNs registration,
payloads and preferences remain unchanged. Each backend supplies its own Firebase
project and server credentials; no publisher relay or shared publisher credential
is needed. Configured metadata means a validated local sender, not demonstrated
Google reachability or device delivery.

## Runtime configuration

Apply database migrations before starting the API or notification sidecar. Set
`SNAPPER_ANDROID_PUSH_CONFIG_FILE` to an explicitly provisioned, private JSON file
in each process that should serve Android metadata or send Android notifications.
An empty variable leaves that process Android-disabled and retains APNs defaults.
It does not retire shared configuration or stop another configured sender.

For container deployments, provision read-only mounts for the runtime JSON and
its referenced private credential file into the API and notification processes
that need them. Mount paths are deployment-specific; setting the environment
variable does not create mounts or copy credentials.

Read the current operator CAS state without changing it:

```sh
snapper android-push-state
```

The result contains `revision` and `active_configuration_id`, both initially null.
Copy the observed revision into `expected_configuration_revision` when preparing
an activation or disable decision. A first activation can use:

```json
{
    "enabled": true,
    "apns_enabled": false,
    "expected_configuration_revision": null,
    "credentials_file": "private/service-account.json",
    "configuration": {
        "configuration_id": "11111111-1111-4111-8111-111111111111",
        "protocol": "fcm_fid",
        "package_name": "ch.snapper.android",
        "project_id": "your-firebase-project",
        "application_id": "your-registered-firebase-android-app-id",
        "api_key": "your-public-client-api-key",
        "messaging_sender_id": "your-project-sender-id"
    }
}
```

Use a newly generated UUID for the real configuration; these are illustrative
public values. Relative credential paths resolve from the runtime JSON directory,
not the current working directory. The service-account file must authorize
Firebase Messaging for the same project. OAuth uses the fixed Google token
endpoint and Firebase Messaging scope. No ambient credential discovery occurs.
Never commit the private file or include it in an image. Credential validation
errors omit raw private input. Metadata exposes only public Firebase options.

Register the actual Android package in the backend owner's Firebase project.
Optional Android API-key restrictions must match the installed APK's app-signing
certificate, including the Play app-signing certificate for Play distribution.
The upload certificate is a separate key. Public client options do not grant
permission to send messages.

API and `snapper notify` startup validate the sender before atomically activating
the configuration. Same active UUID and identical public options are restart
idempotent. Change the UUID when public options change; credential-only rotation
may keep it. Retired UUIDs never reactivate. A competing or stale predecessor
fails startup rather than overwriting a newer decision.

For a global disable, read the current revision and provision explicit JSON with
`enabled:false` and that `expected_configuration_revision`. This atomically
retires the active configuration and its Android bindings and records a disabled
head. Initial null is never valid again after any decision, including initial
disable. Read the new revision before another decision or reapplication of a
disable. Removing a process's filename is only a local disable.

## Registration and delivery

Authenticated Android clients read `/api/devices/android/config`, then prepare
`/api/devices/android/registration-state?configuration_id=...&installation_id=...`.
Registration uses `POST /api/devices/android` with CSRF, explicit `fcm_fid`, the
captured installation revision, expected user and a fresh binding ID. No trading
permission or wallet is required. Registration FIDs are never returned by inventory.
The legacy APNs list excludes Android rows; Android inventory uses
`GET /api/devices/android`. Existing owned delete and preference endpoints support
both providers and retain all seven notification types.

Same-installation replacement requires the current revision. A delayed first
submission cannot retire a newer binding. Retired bindings never reactivate;
another installation claiming an active FID receives a conflict without takeover
or owner disclosure. Mutations are not automatically replayed by the client.

FCM HTTP v1 sends `message.fid` and data-only opaque registration, binding,
configuration and alert IDs with a fixed 24-hour expiry. No account or financial
content is sent to the provider. Every attempt revalidates current user,
registration, configuration, captured scope, preferences and authority. Retry
and terminal transitions compare the charged attempt count. Provider acceptance
is recorded separately from APNs IDs and is not proof of device delivery.

Only explicit FCM `UNREGISTERED` invalidates a binding. Other permanent 4xx errors
do not; 429/5xx retry within the original expiry and three-attempt budget.
Retry-After is respected. A message 401 clears its OAuth token for a later hint
but does not replay that message. Retired configurations fail subsequent admission
checks, and workers cannot retarget an old outbox row to a replacement registration.
Admission checks cannot recall a provider request already in flight and do not
guarantee exactly-once delivery.

Native handling requires explicit consent, notification permission and the current
accepted durable binding. It may show only a generic invitation to open Snapper.
Private content and navigation wait for current foreground authentication and
permission checks. A background service must not read protected session cookies
or construct an authenticated repository. One Firebase project per process remains
the conservative lifecycle boundary.

## Validation boundary

Automated tests use disposable SQLite databases, generated synthetic signing keys
and in-process fake HTTP/providers. They do not send to Google or configure a
phone. Real FID registration, killed-process delivery, offline cleanup and
cross-project process lifecycle must be validated with disposable Firebase projects
before claiming working device delivery. Backend availability alone does not prove
those behaviors.

Primary references: [HTTP v1 authorization](https://firebase.google.com/docs/cloud-messaging/send/v1-api),
[FID message target](https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages),
[FCM error handling](https://firebase.google.com/docs/cloud-messaging/error-codes),
[runtime Firebase options](https://firebase.google.com/docs/projects/multiprojects).
