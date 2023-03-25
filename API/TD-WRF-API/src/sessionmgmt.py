""" Session management module """
import string
import random
import redis


r = redis.Redis(host='localhost', port=6379, db=0)
SESSION_TIME = 24 * 60 * 60


class CreateSession:
    def __init__(self, user):
        self.user = user

    @staticmethod
    def id_generator(size=6):
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(size))

    def generate_session(self):
        session_id = self.id_generator(size=24)
        r.setex(self.user, SESSION_TIME, "session_id " + session_id)
        return True

    def get_session_id(self):
        session_id = r.get(self.user).split()[1].decode('ascii')
        r.expire(self.user, SESSION_TIME)
        return session_id


class ValidateSession:
    def __init__(self, header, user):
        self.header = header
        self.user = user
        self.key = header.get('access-key')

    @staticmethod
    def error_message_api(message, status_code):
        return {"error": message,
                    "status_code": status_code}

    def validate(self):
        cs = CreateSession(self.user)
        session_id = cs.get_session_id()
        if self.key is not None:
            if self.key == session_id:
                return True, self.error_message_api(message="Session validated successfully",
                                                    status_code=200)
            else:
                return False, self.error_message_api(message="Session Expired",
                                                     status_code=403)
        else:
            return False, self.error_message_api(message="API key is missing",
                                                 status_code=403)
