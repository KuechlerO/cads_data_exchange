import datetime
from dataclasses import dataclass
from unidecode import unidecode


class NameInfoException(Exception):
    pass


@dataclass(frozen=True, eq=True)
class NameInfo:
    first: str
    last: str
    birthdate: datetime.date

    @classmethod
    def from_any(cls, first, last, birthdate):
        if first is None:
            first = ""
        if last is None:
            last = ""

        first = unidecode(first).strip().replace("-", " ").lower().split()
        last = unidecode(last).strip().replace("-", " ").lower().split()
        if type(birthdate) is str:
            if "-" in birthdate:
                birthdate = datetime.datetime.strptime(birthdate, "%Y-%m-%d").date()
            elif "." in birthdate:
                birthdate = datetime.datetime.strptime(birthdate, "%d.%m.%Y").date()
            else:
                raise NameInfoException(f"Invalid date string: {birthdate} from {last}, {first}")
        return cls(first=first, last=last, birthdate=birthdate)

    def match(self, other):
        birthdate_valid = (self.birthdate == other.birthdate) or not self.birthdate

        name_valid = False
        if not self.first or not self.last or not other.first or not other.last:
            name_valid = False
        elif self.first == other.first and self.last == other.last:
            name_valid = True
        elif self.first[0] == other.first[0] and self.last == other.last:
            name_valid = True

        return birthdate_valid and name_valid
