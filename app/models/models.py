class Abbreviation:
    def violation_type(self, violation_code):
        if violation_code == "0":
            return "C0"
        elif violation_code == "1":
            return "C1"
        elif violation_code == "2":
            return "C2"
        elif violation_code == "3":
            return "C3"

        elif violation_code == "4":
            return "C4"
        elif violation_code == "5":
            return "C5"
        elif violation_code == "6":
            return "C6"
        elif violation_code == "7":
            return "C7"
        elif violation_code == "8":
            return "C8"
        elif violation_code == "9":
            return "C9"
        elif violation_code == "11":
            return "C11"
        elif violation_code == "13":
            return "C16-пассажир"
        elif violation_code == "16":
            return "C16-водитель"
        elif violation_code == '32':
            return "C16-телефон"

        else:
            return "unknown"

    def get_vehicle_type(vehicle_code):
        if vehicle_code == 1:
            return "car"
        elif vehicle_code == 2:
            return "truck"
        elif vehicle_code == 3:
            return "bus"

        else:
            return "unknown"


    def get_country_code(country_code):
        if country_code == "rus":
            return "RU"
        if country_code == "blr":
            return "BY"
        if country_code == "kaz":
            return "KZ"
        if country_code == "arm":
            return "AM"
        if country_code == "AZE":
            return "AZ"
        if country_code == "UZB":
            return "UZ"
        if country_code == "kgz":
            return "KG"
        if country_code == "blr":
            return "BY"

