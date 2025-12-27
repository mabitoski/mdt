# MDT Web

Appli web + API d'ingestion pour afficher les informations remontees par MDT (MAC, serial, etat des composants, etc.).

## Demarrage

```bash
npm install
npm start
```

L'UI est disponible sur `http://localhost:3000`.

Prerequis : PostgreSQL (definis `DATABASE_URL` ou les variables `PG*`).

## Docker

Le plus simple est d'utiliser Docker Compose (PostgreSQL inclus) :

```bash
docker compose up -d
```

Ou en mode `docker run` si tu as deja un PostgreSQL accessible :

```bash
docker build -t mdt-web .
docker run -d --name mdt-web -p 3000:3000 \
  -e DATABASE_URL=postgres://mdt:mdt@localhost:5432/mdt \
  -e SESSION_SECRET=change-me \
  -e COOKIE_SECURE=0 \
  mdt-web
```

## API

### POST `/api/ingest`

Endpoint public (sans authentification) pour recevoir les donnees MDT.

Exemple :

```bash
curl -X POST http://localhost:3000/api/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "hostname": "PC-001",
    "macAddress": "AA:BB:CC:DD:EE:FF",
    "macAddresses": ["AA:BB:CC:DD:EE:FF", "11:22:33:44:55:66"],
    "serialNumber": "ABC123",
    "category": "laptop",
    "vendor": "Dell",
    "technician": "Remi",
    "model": "Latitude 5420",
    "osVersion": "Windows 11",
    "ramGb": 16,
    "ramSlotsTotal": 2,
    "ramSlotsFree": 1,
    "batteryHealth": 87,
    "cameraStatus": "ok",
    "usbStatus": "ok",
    "keyboardStatus": "ok",
    "padStatus": "ok",
    "badgeReaderStatus": "absent",
    "components": {
      "disk": "ok",
      "ram": "ok",
      "battery": "good"
    }
  }'
```

Champs acceptes (tous optionnels sauf identifiant) :
- `hostname`, `macAddress`, `serialNumber` (au moins un requis)
- `macAddresses` : liste de MAC (toutes interfaces)
- `category` : `laptop`, `desktop` (ou `portable`, `tour`, etc.)
- `vendor`, `model`, `osVersion`
- `technician` : prenom/identifiant du technicien
- `ramMb` / `ramGb`, `ramSlotsTotal`, `ramSlotsFree`
- `batteryHealth` (0-100)
- `cameraStatus`, `usbStatus`, `keyboardStatus`, `padStatus`, `badgeReaderStatus` (`ok`, `nok`, `absent`)
- `components` : objet cle/valeur (etat des composants)

Ces champs peuvent aussi etre envoyes dans un objet `components` ou `hardware` si c'est plus simple pour les scripts MDT.

Reponse :

```json
{ "ok": true, "id": 1, "machineKey": "sn:ABC123" }
```

### GET `/api/machines`

Liste des machines (dernier passage MDT).

### GET `/api/machines/:id`

Detail d'une machine (composants + payload complet).

## Donnees

Les donnees sont stockees en PostgreSQL.

## Variables d'environnement

- `PORT` : port HTTP (defaut `3000`)
- `DATABASE_URL` : URL PostgreSQL complete (ex: `postgres://user:pass@host:5432/mdt`)
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` : alternative a `DATABASE_URL`
- `PGSSLMODE` / `PGSSL` : activer TLS pour PostgreSQL
- `PGSSL_REJECT_UNAUTHORIZED=0` : accepte un certificat TLS non valide
- `JSON_LIMIT` : taille max d'un payload JSON (defaut `256kb`)
- `INGEST_RATE_LIMIT` : requetes/minute par IP (defaut `180`)
- `TRUST_PROXY` : `1` si l'app est derriere un reverse proxy
- `SESSION_SECRET` : secret des sessions (defaut auto, a fixer en prod)
- `SESSION_NAME` : nom du cookie de session (defaut `mdt.sid`)
- `COOKIE_SECURE` : `1` pour forcer le cookie secure (HTTPS uniquement)

## Authentification

L'UI et les endpoints de lecture sont proteges par session. L'endpoint d'ingestion reste ouvert.

Compte local par defaut :
- `admin` / `admin` (changeable via `LOCAL_ADMIN_USER` et `LOCAL_ADMIN_PASSWORD`)
- `ALLOW_LOCAL_ADMIN=0` pour desactiver le compte local

LDAP (optionnel) :
- `LDAP_URL` : ex `ldap://ad.local:389`
- `LDAP_BIND_DN` et `LDAP_BIND_PASSWORD` : optionnels si bind anonyme
- `LDAP_SEARCH_BASE` : ex `DC=ad,DC=local`
- `LDAP_SEARCH_FILTER` : defaut `(sAMAccountName={{username}})`
- `LDAP_SEARCH_ATTRIBUTES` : defaut `dn,cn,mail`
- `LDAP_TLS_REJECT_UNAUTHORIZED=0` pour accepter un certificat non valide

## Notes securite

L'endpoint d'ingestion est volontairement ouvert. Pour un usage en production :
- place l'app derriere un VPN ou une IP allowlist,
- ajuste `INGEST_RATE_LIMIT` selon ton volume MDT,
- surveille l'espace disque si tu stockes beaucoup de payloads.

## Scripts PowerShell (MDT)

Les scripts sont dans `scripts/` :
- `mdt-report.ps1` : detection auto de la categorie + tests materiel
- `mdt-laptop.ps1` : force la categorie `laptop`
- `mdt-desktop.ps1` : force la categorie `desktop`
- `mdt-stress.ps1` : lance les tests en mode `stress` (boucles WinSAT)
- `keyboard_capture.ps1` : outil graphique pour valider le clavier (Windows)

Exemples :

```powershell
.\scripts\mdt-report.ps1 -ApiUrl "http://serveur-mdt:3000/api/ingest"
.\scripts\mdt-laptop.ps1 -ApiUrl "http://serveur-mdt:3000/api/ingest" -TestMode quick
.\scripts\mdt-desktop.ps1 -ApiUrl "http://serveur-mdt:3000/api/ingest" -TestMode stress
.\scripts\mdt-stress.ps1 -ApiUrl "http://serveur-mdt:3000/api/ingest" -Category laptop
```

Options utiles :
- `-SkipTlsValidation` : accepte un certificat TLS invalide
- `-TimeoutSec 15` : timeout HTTP
- `-TestMode` : `none`, `quick`, `stress` (par defaut `quick`)
- `-DiskTestTimeoutSec` / `-MemTestTimeoutSec` : timeout des tests WinSAT
- `-CpuTestTimeoutSec` / `-GpuTestTimeoutSec` : timeout WinSAT CPU/GPU
- `-StressLoops` : nombre de boucles en mode `stress`
- `-FsCheckMode` : `auto` (stress uniquement), `scan`, `none`
- `-FsCheckTimeoutSec` : timeout du `chkdsk /scan`
- `-MemDiagMode` : `none` ou `schedule` (planifie `mdsched`)
- `-CameraTestPath` : chemin du binaire de test camera (exit code 0 = ok)
- `-CameraTestTimeoutSec` : timeout du test camera (defaut `20`)
- `-CpuTestPath` / `-GpuTestPath` : binaire externe de stress CPU/GPU (optionnel)
- `-CpuTestArguments` / `-GpuTestArguments` : arguments des binaires externes
- `-NetworkTestPath` : binaire `iperf3` (optionnel)
- `-NetworkTestServer` / `-NetworkTestPort` : serveur iperf
- `-NetworkTestSeconds` / `-NetworkTestTimeoutSec` : duree/timeout iperf
- `-NetworkTestDirection` : `download`, `upload`, `both`
- `-NetworkTestExtraArgs` : arguments supplementaires iperf
- `-NetworkPingTarget` / `-NetworkPingCount` : ping rapide (par defaut gateway)
- `-MsinfoTimeoutSec` : timeout de msinfo32 si fallback MAC (defaut `0`, desactive par defaut)
- `-MacPreference` : `auto`, `ethernet`, `wifi`, `any` (defaut `auto`)
- `-LogPath` : chemin du fichier de log (defaut `scripts/mdt-report.log`)
- `-Technician` : nom/prenom du technicien (ou env `MDT_TECHNICIAN`)
- `-SkipKeyboardCapture` : ne lance pas `keyboard_capture.ps1` sur les laptops
- `-KeyboardCapturePath` / `-KeyboardCaptureLogPath` / `-KeyboardCaptureConfigDir` : options du script clavier
- `-KeyboardCaptureLayout` / `-KeyboardCaptureLayoutConfig` / `-KeyboardCaptureBlockInput` : layout et blocage des touches

Notes clavier :
- `mdt-report.ps1` lance `keyboard_capture.ps1` a la fin si la categorie est `laptop`.

Notes tests :
- Les tests utilisent WMI/CIM et WinSAT (disponible en OS complet).
- En WinPE, certains tests peuvent renvoyer `absent`.
