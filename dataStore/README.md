# dataStore

Enthält in Unterordnern

- die NEW Fälle (cases, deaths, recovered) für districts und states des heutigen Tages als JSON Datei
- die accumulierten Fälle (cases, deaths, recovered) für districts und states seit dem Beginn der Pandemie als JSON Datei
- die historische dynamische Entwicklung (cases, deaths, recovered) für districts und states als JSON Datei
- die frozen-incidence history Werte für seit letztem Montag als JSON Datei, und in einem Unterordner csv die csv Dateinen der vergangen 30 Tage. Die csv Daten gibt es nicht im Docker Image!
- die meta Daten des zuletzt verwendeten Dumps des RKI

Quellenvermerk:

- RKI (https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Daten.html)
