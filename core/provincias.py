from bunch import Bunch


def get_provincias():
    return Bunch(**{
        k: Bunch(nombre=v) for k, v in (
            ("01", "Araba/Álava"),
            ("02", "Albacete"),
            ("03", "Alicante/Alacant"),
            ("04", "Almería"),
            ("05", "Ávila"),
            ("06", "Badajoz"),
            ("07", "Balears, Illes"),
            ("08", "Barcelona"),
            ("09", "Burgos"),
            ("10", "Cáceres"),
            ("11", "Cádiz"),
            ("12", "Castellón/Castelló"),
            ("13", "Ciudad Real"),
            ("14", "Córdoba"),
            ("15", "Coruña, A"),
            ("16", "Cuenca"),
            ("17", "Girona"),
            ("18", "Granada"),
            ("19", "Guadalajara"),
            ("20", "Gipuzkoa"),
            ("21", "Huelva"),
            ("22", "Huesca"),
            ("23", "Jaén"),
            ("24", "León"),
            ("25", "Lleida"),
            ("26", "Rioja, La"),
            ("27", "Lugo"),
            ("28", "Madrid"),
            ("29", "Málaga"),
            ("30", "Murcia"),
            ("31", "Navarra"),
            ("32", "Ourense"),
            ("33", "Asturias"),
            ("34", "Palencia"),
            ("35", "Palmas, Las"),
            ("36", "Pontevedra"),
            ("37", "Salamanca"),
            ("38", "Santa Cruz de Tenerife"),
            ("39", "Cantabria"),
            ("40", "Segovia"),
            ("41", "Sevilla"),
            ("42", "Soria"),
            ("43", "Tarragona"),
            ("44", "Teruel"),
            ("45", "Toledo"),
            ("46", "Valencia/València"),
            ("47", "Valladolid"),
            ("48", "Bizkaia"),
            ("49", "Zamora"),
            ("50", "Zaragoza"),
            ("51", "Ceuta"),
            ("52", "Melilla"),
        )
    })


def prov_to_cod(p):
    p = p.upper()
    for a, b in (
        ("Á", "A"),
        ("É", "E"),
        ("Í", "I"),
        ("Ó", "O"),
        ("Ú", "U"),
    ):
        p = p.replace(a, b)
    p = p.split("/")[0].strip()
    if p in ("A CORUÑA", "CORUÑA, A"):
        return "15"
    if p == "ALBACETE":
        return "02"
    if p == "ALICANTE":
        return "03"
    if p == "ALMERIA":
        return "04"
    if p in ("ARABA", "ALAVA"):
        return "01"
    if p == "ASTURIAS":
        return "33"
    if p == "AVILA":
        return "05"
    if p == "BADAJOZ":
        return "06"
    if p == "BARCELONA":
        return "08"
    if p == "BIZKAIA":
        return "48"
    if p == "BURGOS":
        return "09"
    if p == "CACERES":
        return "10"
    if p == "CADIZ":
        return "11"
    if p == "CANTABRIA":
        return "39"
    if p == "CASTELLON":
        return "12"
    if p == "CEUTA":
        return "51"
    if p == "CIUDAD REAL":
        return "13"
    if p == "CORDOBA":
        return "14"
    if p == "CUENCA":
        return "16"
    if p == "GIPUZKOA":
        return "20"
    if p == "GIRONA":
        return "17"
    if p == "GRANADA":
        return "18"
    if p == "GUADALAJARA":
        return "19"
    if p == "HUELVA":
        return "21"
    if p == "HUESCA":
        return "22"
    if p in ("ILLES BALEARS", "BALEARS, ILLES"):
        return "07"
    if p == "JAEN":
        return "23"
    if p in ("LA RIOJA", "RIOJA, LA"):
        return "26"
    if p in ("LAS PALMAS", "PALMAS, LAS"):
        return "35"
    if p == "LEON":
        return "24"
    if p == "LLEIDA":
        return "25"
    if p == "LUGO":
        return "27"
    if p == "MADRID":
        return "28"
    if p == "MALAGA":
        return "29"
    if p == "MELILLA":
        return "52"
    if p == "MURCIA":
        return "30"
    if p == "NAVARRA":
        return "31"
    if p == "OURENSE":
        return "32"
    if p == "PALENCIA":
        return "34"
    if p == "PONTEVEDRA":
        return "36"
    if p == "SALAMANCA":
        return "37"
    if p == "SEGOVIA":
        return "40"
    if p == "SEVILLA":
        return "41"
    if p == "SORIA":
        return "42"
    if p in ("STA. CRUZ DE TENERIFE", "SANTA CRUZ DE TENERIFE"):
        return "38"
    if p == "TARRAGONA":
        return "43"
    if p == "TERUEL":
        return "44"
    if p == "TOLEDO":
        return "45"
    if p == "VALENCIA":
        return "46"
    if p == "VALLADOLID":
        return "47"
    if p == "ZAMORA":
        return "49"
    if p == "ZARAGOZA":
        return "50"
    return None


def normalizarProvincia(s):
    s = s.strip()
    for t in (
        ("Alava", "Araba/Álava"),
        ("Alicante", "Alicante/Alacant"),
        ("Balears, Illes", "Balears (Illes)", "Baleares Illes", "Balears Illes"),
        ("Castellón", "Castellón/Castelló", "Castellón de la Plana"),
        ("Coruña, A", "Coruña (La)", "Coruña (A)"),
        ("Gipuzkoa", "Guipúzcoa"),
        ("Rioja, La", "Rioja (La)"),
        ("Madrid", "Madrid, Comunidad de"),
        ("Ávila", "Avila"),
        ("Murcia", "Murcia, Región de"),
        ("Navarra", "Navarra, Comunidad Foral de"),
        ("Asturias", "Asturias, Principado de"),
        ("Palmas (Las)", "Palmas, Las"),
        ("Valencia", "Valencia/València", "Valencia\\València"),
        ("Bizkaia", "Vizcaya")
    ):
        if s in t:
            return t[0]
    return s
