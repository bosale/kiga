Kindergarten Datenextraktion
==========================

Übersicht
---------
Dieses Projekt ist ein Python-basiertes Tool zur automatisierten Extraktion und Verarbeitung von Daten aus Excel-Dateien für Kindergärten. Es ermöglicht die systematische Erfassung verschiedener Aspekte des Kindergartenbetriebs, wie Elternbeiträge, Personalausgaben, Sachausgaben und weitere wichtige Kennzahlen.

Projektstruktur
--------------
Das Projekt ist wie folgt strukturiert:

/01_src/           - Quellcode des Projekts
  /config/         - YAML-Konfigurationsdateien
  /extractors/     - Verschiedene Datenextraktoren
  /utils/          - Hilfsfunktionen und Utilities
/02_data/          - Ein- und Ausgabedaten
/03_logs/          - Protokolldateien

Hauptfunktionen
--------------
Das Tool unterstützt die Extraktion folgender Daten:
- Deckblatt-Informationen
- Elternbeiträge
- Zusatzangaben
- Schließzeiten
- Öffnungszeiten
- Verpflegung
- Anlagenverzeichnis
- Verteilungsschlüssel
- Personalausgaben
- Sachausgaben
- Einnahmen
- Vermögensübersicht
- Verbindlichkeiten

Installation
-----------
1. Python 3.x wird benötigt
2. Virtuelle Umgebung erstellen:
   python -m venv .venv
3. Virtuelle Umgebung aktivieren:
   - Windows: .venv\Scripts\activate
   - Linux/Mac: source .venv/bin/activate
4. Abhängigkeiten installieren:
   pip install -r requirements.txt

SQL Server Integration
--------------------
Das Tool unterstützt die automatische Speicherung der extrahierten Daten in einer SQL Server-Datenbank. Die Daten werden sowohl als CSV-Dateien als auch in der Datenbank gespeichert.

Konfiguration:
1. SQL Server-Verbindung in `01_src/config.json` konfigurieren:
   ```json
   {
       "server": "your_server",
       "database": "your_database",
       "schema_name": "dbo",
       "driver": "ODBC Driver 17 for SQL Server",
       "trusted_connection": "yes"
   }
   ```

2. Benötigte Komponenten:
   - SQL Server ODBC-Treiber (Version 17 oder höher)
   - SQLAlchemy und pyodbc (in requirements.txt enthalten)

Automatische Typinferenz:
- Das Tool erkennt automatisch die passenden SQL-Datentypen basierend auf den Pandas-Datentypen:
  * Integer → SQL INTEGER
  * Float → SQL FLOAT
  * DateTime → SQL DATETIME
  * Boolean → SQL BOOLEAN
  * Text → SQL VARCHAR(255) standardmäßig
  * Text mit Schlüsselwörtern (beschreibung, erlaeuterung, etc.) → SQL VARCHAR(1000)

- Die Typinferenz kann bei Bedarf überschrieben werden durch Angabe spezifischer SQL-Typen im Code.

Verwendung
---------
Das Hauptskript extract_data.py kann mit verschiedenen Parametern aufgerufen werden:

python 01_src/extract_data.py [Extraktionstyp] [Parameter]

Beispiele:
```bash
# Normale Extraktion (CSV + SQL)
python 01_src/extract_data.py --type deckblatt

# Nur CSV-Export (kein SQL)
python 01_src/extract_data.py --type deckblatt --no-sql

# Debug-Modus (nur eine Datei)
python 01_src/extract_data.py --type deckblatt --debug
```

Parameter:
- --type: Art der zu extrahierenden Daten (erforderlich)
- --input-dir: Eingabeverzeichnis (optional)
- --output-dir: Ausgabeverzeichnis (optional)
- --config: Pfad zur Konfigurationsdatei (optional)
- --debug: Debug-Modus aktivieren (optional)
- --no-sql: SQL-Server-Export überspringen (optional)

Konfiguration
------------
Die Extraktionslogik wird durch YAML-Konfigurationsdateien im Verzeichnis 01_src/config/ gesteuert. Jeder Extraktionstyp hat seine eigene Konfigurationsdatei, die die Struktur der zu extrahierenden Daten definiert.

Logging
-------
Das System erstellt detaillierte Logs im Verzeichnis 03_logs/, die bei der Fehlersuche und Überwachung der Datenextraktion helfen. Die Logs enthalten auch Informationen über die automatisch erkannten SQL-Datentypen.

Fehlerbehandlung
---------------
Problematische Dateien werden dokumentiert und in separaten CSV-Dateien im Verzeichnis 02_data/ aufgelistet.

Technische Anforderungen
----------------------
- Python 3.x
- SQL Server ODBC-Treiber
- Siehe requirements.txt für alle Abhängigkeiten

Sonstiges
----------------------
- Manche Tabellen brauchen explizites Mapping (siehe sql_data_types.py)