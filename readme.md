# Ordnerstruktur
Die einzelnen Dateien entsprechen den jeweiligen Sheets im Excel.

# Konfiguration von neuen Dateien
In input_files.ini

[PFADZUMEXCEL.xls]
Skriptnamen = Tabellenblatt
Skriptnamen = Tabellenblatt
Skriptnamen = Tabellenblatt


Beispiel:
[Jahresabrechnung-bilanzierende_2.xls]
verteilungsschluessel = B_Deckblatt
traegerorganisation = B_Deckblatt
jahr_abrechnung = B_Deckblatt
deckblatt = B_Deckblatt

Erklärung: 
Für die Datei Jahresabrechnung-bilanzierende_2.xls liefert das Excel-Blatt B_Deckblatt die Informationen, die im Skript namens jahr_abrechnung.py benötigt werden. Der Präfix (01_) beim Skriptnamen (hier: jahr_abrechnung.py) wird dabei nicht berücksichtigt. D. h. ABC_jahr_abrechnung.py ist ebenfalls gueltig.

# Ausführung
python <dateiname.py>
Beispiel: python 07_verteilungsschluessel.py

# Installieren von Python-Paketen:
pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org xlrd