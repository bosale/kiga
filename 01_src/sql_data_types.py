from sqlalchemy import types
# Define the SQL data types for each column


sql_types_metadata = {
    'Traegerorganisation': types.String(length=255), # varchar(255)
    'Jahr_Abrechnung': types.Integer(),              # int
    'Ebene_1': types.String(length=255),             # varchar(255)
    'Ebene_2': types.String(length=255),             # varchar(255)
    'Ebene_3': types.String(length=255),             # varchar(255)
    'Name_Eintrag': types.String(length=255),        # varchar(255)
    'Eintrag': types.String(length=1000),            # varchar(1000)
    'Erlaeuterung': types.String(length=255)         # varchar(255)
}

sql_types_verteilungsschluessel = {
    'Jahr_Abrechnung': types.Integer(),              # int
    'Traegerorganisation': types.String(length=255), # varchar(255)
    'Ebene_1': types.String(length=255),             # varchar(255)
    'Ebene_2': types.String(length=255),             # varchar(255)
    'Ebene_3': types.String(length=255),             # varchar(255)
    'Jahr': types.String(length=255),                         
    'Verteilungsschluessel': types.String(length=255),# varchar(255)
    'Quelle': types.String(length=255)               # varchar(255)
}
