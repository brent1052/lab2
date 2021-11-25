from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class FutureResidenceDetails(Base):
    """ Future Residence Details """

    __tablename__ = "future_residence_details"

    id = Column(Integer, primary_key=True)
    user_id = Column(String(250), nullable=False)
    user_name = Column(String(250), nullable=False)
    residence_type = Column(String(100), nullable=False)
    phone_num = Column(String(100), nullable=False)
    capacity = Column(Integer, nullable=False)
    date = Column(String(100), nullable=False)
    date_created = Column(DateTime, nullable=False)

    def __init__(self, user_id, user_name, residence_type, phone_num, capacity, date):
        """ Initializes a schedule residence details request """
        self.user_id = user_id
        self.user_name = user_name
        self.residence_type = residence_type
        self.phone_num = phone_num
        self.capacity = capacity
        self.date = date
        self.date_created = datetime.datetime.now()

    def to_dict(self):
        """ Dictionary Representation of a scheduled residence details request """
        dict = {}
        dict['id'] = self.id
        dict['user_id'] = self.user_id
        dict['user_name'] = self.user_name
        dict['residence_type'] = self.residence_type
        dict['phone_num'] = self.phone_num
        dict['capacity'] = self.capacity
        dict['date'] = self.date
        dict['date_created'] = self.date_created

        return dict