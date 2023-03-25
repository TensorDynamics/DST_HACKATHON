""" Password management Module"""

import bcrypt
import pandas as pd

from src.sessionmgmt import CreateSession


class PasswordManager:
    def __init__(self, user, password):
        self.user = user
        self.password = password
        self.session = None
        # Since End point to generate and select password is not enabled. In
        # this case we need to generate everything manually and paste in the
        # excel. Going forwards we need to do away with this restriction.
        # self.salt = bcrypt.gensalt()
        # self.hashed_password = bcrypt.hashpw(self.password.encode('utf-8'),
        #                                      self.salt).decode('utf-8')
        # self.salt = self.salt.decode('utf-8')

    @staticmethod
    def messageformat(message, status_code):
        return {"error": message, "status_code": status_code}

    @classmethod
    def fromrequest(cls, request):
        """
        Ideally this should parse all type of request objects but here I am
        just implementing the parse from the form data.
        :param request: request object
        :return: validated Auth token
        """
        username = request.json['user']
        password = request.json['password']
        return cls(username, password)

    def validate(self):
        if self.user is None or self.password is None:
            return False, "User id or password is not valid. Login unsuccessful."
        else:
            all_data = pd.read_excel('user.xlsx', dtype=str)
            if self.user in all_data['userid'].to_list():
                user_record = all_data[all_data['userid'] == self.user]
                user_pass = user_record['password'].values[0]
                # check the hashed password
                check = bcrypt.checkpw(self.password.encode('utf-8'),
                                       user_pass.encode('utf-8'))
                if check:
                    self._generate_session()
                    return True, self.session
                else:
                    return False, "User-id & password mismatch"
            else:
                return False, "User-id not found."

    def _generate_session(self):
        cs = CreateSession(self.user)
        try:
            self.session = cs.get_session_id()
        except AttributeError:
            cs.generate_session()
            self.session = cs.get_session_id()
        return self.session
