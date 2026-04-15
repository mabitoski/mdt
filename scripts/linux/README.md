# Scripts Linux Serveur

Ces scripts envoient un rapport Linux categorie `server` vers MMA Web via `POST /api/ingest`.

## Fichiers

- `mma-server-report.sh` : collecte un rapport serveur et l'envoie a MMA
- `mma-server-runner.sh` : wrapper local qui archive le payload JSON et le log d'execution
- `mma-server.env.sample` : exemple de configuration a sourcer

## Donnees remontees

- identifiants : hostname, serial, MAC, vendor, model, OS
- materiel : CPU, GPU si present, disques, volumes, baseboard, RAM si `dmidecode` est disponible
- etat : ping reseau, check fichiersystems, services systemd en echec, RAID logiciel `/proc/mdstat`, thermique si disponible

## Usage simple

```bash
cd /home/linus/mdt-web/scripts/linux
cp mma-server.env.sample mma-server.env
chmod +x mma-server-report.sh mma-server-runner.sh
./mma-server-runner.sh
```

## Execution manuelle sans envoi

```bash
./mma-server-report.sh --dry-run --output /tmp/mma-server-payload.json
```

## Notes

- `curl` est requis pour l'envoi
- `dmidecode`, `smartctl`, `lspci`, `ip`, `findmnt`, `systemctl` sont optionnels mais enrichissent le payload
- pour un inventaire DMI complet et `smartctl`, execute de preference en root
