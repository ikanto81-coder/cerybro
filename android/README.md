# Cerybro Android (TWA)
Trusted Web Activity around https://cerybro.cairakuzey.cloud (real Chrome: Google login + passkeys work).
`LaunchAppActivity` opens installed launcher apps by fuzzy name; the web app calls it with
`intent://launch?name=X#Intent;scheme=cerybro;package=com.cairakuzey.cerybro;end`.

Build (JDK 17, Android SDK 35):
    CERYBRO_KEYSTORE_PROPS=/path/to/keystore.properties ./gradlew assembleRelease
Keystore lives only on the VPS at /opt/ai-ceo/android/keystore (backup: /opt/ai-ceo/backups/android-keystore). Never commit it.
Digital Asset Links: /.well-known/assetlinks.json (copy in deploy/android/).
APK links: /opt/ai-ceo/download/<random>/cerybro.apk, removed after 24h by deploy/cron/cerybro-apk-links.
