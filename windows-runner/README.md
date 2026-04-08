# MMA MDT Runner

Application Windows locale pour relancer les checks MDT sans reboot PXE.

## Objectif

- relancer tous les checks atelier depuis Windows
- choisir le technicien manuellement a chaque lancement
- stocker le rapport localement si l'API MMA est indisponible
- resynchroniser plus tard la file locale
- declencher le reset usine uniquement si l'operateur le coche

## Fonctionnement

```text
Operateur lance l'app
-> choisit technicien / type / mode
-> l'app execute mdt-report.ps1 localement
-> si l'API repond: envoi immediat
-> sinon: payload + artefacts copies dans l'outbox locale
-> un bouton ou une tache planifiee rejoue l'outbox plus tard
```

## Fichiers principaux

- `MmaMdtRunner.ps1` : UI WinForms
- `MmaMdtRunner.Common.ps1` : fonctions partagees
- `Register-MmaMdtRunnerSyncTask.ps1` : tache planifiee de sync
- `config.sample.json` : configuration par defaut
- `technicians.json` : liste locale des techniciens
- `../scripts/mdt-report.ps1` : moteur de checks
- `../scripts/mdt-outbox-sync.ps1` : synchronisation de la file offline

## Outbox

Emplacement par defaut :

`%ProgramData%\MMA\MdtRunner\Outbox`

Structure :

- `pending\<reportId>\payload.json`
- `pending\<reportId>\meta.json`
- `pending\<reportId>\artifacts\...`
- `sent\...`
- `failed\...`

## Build MSI

### Build Linux

Prerequis :

- `wixl`
- `msitools`

Commande :

```bash
cd windows-runner
./build-msi.sh 1.0.0
```

### Build Windows

Prerequis :

- Windows
- PowerShell 5.1+
- WiX Toolset v4/v5 (`wix`)

Commande :

```powershell
cd windows-runner
.\build-msi.ps1 -Version 1.0.0
```

## Installation

Le MSI installe :

- l'application dans `C:\Program Files\MMA Automation\MdtRunner`
- les scripts necessaires dans `...\scripts`
- un raccourci bureau `Relancer diagnostic MDT`

## Post-install recommande

Pour la sync automatique de la file :

```powershell
cd "C:\Program Files\MMA Automation\MdtRunner"
powershell -ExecutionPolicy Bypass -File .\Register-MmaMdtRunnerSyncTask.ps1
```
