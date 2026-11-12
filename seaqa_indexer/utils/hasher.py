import base64
import hashlib

try:
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
except ImportError:
    AES = None
    get_random_bytes = None

from seaqa_indexer.config import SECRET_KEY


class AESPasswordDecodeError(Exception):
    pass


BLOCK_SIZE = 32
PADDING = '{'


class AESPasswordHasher:
    algorithm = 'aes-gcm'
    legacy_algorithm = 'aes'

    def __init__(self, secret=None):
        if not secret:
            secret = SECRET_KEY[:BLOCK_SIZE]
        self.secret = secret.encode('utf-8')
        self.key = hashlib.sha256(self.secret).digest()

    def encode(self, password):
        password = _force_str(password).encode('utf-8')
        nonce = get_random_bytes(12)
        cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
        ciphertext, tag = cipher.encrypt_and_digest(password)
        parts = (nonce, tag, ciphertext)
        data = '$'.join(base64.b64encode(part).decode('ascii') for part in parts)
        return "%s$%s" % (self.algorithm, data)

    def verify(self, password, encoded):
        return self.decode(encoded) == password

    def decode(self, encoded):
        try:
            algorithm, data = encoded.split('$', 1)
            if algorithm == self.algorithm:
                nonce, tag, ciphertext = (
                    base64.b64decode(part) for part in data.split('$')
                )
                cipher = AES.new(self.key, AES.MODE_GCM, nonce=nonce)
                return cipher.decrypt_and_verify(ciphertext, tag).decode('utf-8')
            if algorithm == self.legacy_algorithm:
                cipher = AES.new(self.secret, AES.MODE_ECB)
                plaintext = cipher.decrypt(base64.b64decode(data))
                return plaintext.decode('utf-8').rstrip(PADDING)
        except (ValueError, TypeError, UnicodeDecodeError) as error:
            raise AESPasswordDecodeError from error

        raise AESPasswordDecodeError


def _force_str(s, encoding='utf-8', strings_only=False, errors='strict'):
    if strings_only and isinstance(s, (bytes, bytearray)):
        raise TypeError("Cannot handle bytes when strings_only is set to True.")
    if isinstance(s, bytes):
        return s.decode(encoding, errors)
    elif isinstance(s, bytearray):
        return s.decode(encoding, errors).encode(encoding)
    elif isinstance(s, (list, tuple)):
        return [_force_str(item, encoding, strings_only, errors) for item in s]
    elif isinstance(s, dict):
        return {_force_str(key, encoding, strings_only, errors): _force_str(value, encoding, strings_only, errors) for key, value in s.items()}
    else:
        return str(s)
