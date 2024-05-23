import hashlib
import boto3
import base64
import secrets
import chardet
from cryptography.hazmat.primitives.ciphers import Cipher as C
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.primitives.padding import PKCS7


class Cipher:
    # jason.chia: awskms cipher + hash class
    # see sanity test script below for example usage

    def __init__(self, kms_key_id, salt, codec='utf-8'):
        # jason.chia: we don't initialize csv specific stuff here to keep this class generic enough to use on other applications
        self.kms_key_id = kms_key_id
        self.dek = None
        self.edek = None
        self.salt = salt
        self.codec = codec
        if isinstance(self.salt, str):
            self.salt = self.salt.encode(self.codec)

    def _refresh_dek(self):
        c = boto3.client('kms')
        res = c.generate_data_key(
            KeyId=self.kms_key_id,
            KeySpec='AES_128'
        )
        self.dek = res['Plaintext']
        self.edek = res['CiphertextBlob']

    def initialize_as_csv_processor(self, headers, fields, delimiter, slow_hashing = None):
        # initialize this cipher as a csv processor
        # process_line will treat each line as a csv line
        self._pcid = []
        self._shcid = []
        self._delimiter = delimiter.encode(self.codec)
        self._headers = headers
        for f in fields:
            # obtain the column index to process
            self._pcid.append(headers.index(f))

        if isinstance(slow_hashing, str):
            slow_hashing = [slow_hashing]
        if not isinstance(slow_hashing, list):
            slow_hashing = []

        for f in slow_hashing:
            # obtain slow hash column index to process
            idx = headers.index(f)
            if idx not in self._pcid:
                raise Exception('field %s specified for slow hashing but not specified for encryption' % (f))
            self._shcid.append(idx)

        if not isinstance(self.dek, bytes):
            self._refresh_dek()

        self.process_line = self._process_csv_line

    def _encrypt_field(self, value):
        # this always expects bytes and return bytes
        iv = secrets.token_bytes(16)
        pad = PKCS7(AES.block_size).padder()
        enc = C(AES(self.dek), CBC(iv)).encryptor()
        padded = pad.update(value) + pad.finalize()
        ct_bin = enc.update(padded) + enc.finalize()
        return base64.b64encode(iv + ct_bin)

    def _hash_field(self, value):
        # this always expects bytes and return bytes
        return hashlib.sha256(self.salt + value).hexdigest().encode(self.codec)

    def _slow_hash_field(self, value):
        # this always expects bytes and return bytes
        # update 2023 oct 26, to accomodate for FPX daily snapshot ingestion
        # parameters lowered to N=32,
        # originaly estimate for N=256 takes 707 years to exhaust 16 digits for at a rate of 448000 H/s
        # now with N=32, assuming rate of 3584000 H/s, it will take 80 years to exhaust 16 digits
        rounds = 5
        n = (1 << rounds)
        r = 8
        p = 1
        dklen = 32
        h = hashlib.scrypt(
            value,
            salt=self.salt,
            n=n,
            r=r,
            p=p,
            dklen=32
        )
        chksum = base64.b64encode(h).rstrip(b'=')
        fullhash = f'$scrypt$ln={rounds},r={r},p={p}$$'.encode(self.codec)
        fullhash += chksum
        return fullhash

    @property
    def aws_enc_ctx(self):
        # obtain the encryption context used for current encryption operations
        part0 = base64.b64encode(self.edek).decode(self.codec)
        return f'{part0}:{self.kms_key_id}'

    @property
    def new_headers(self):
        # the resulting headers if we are initialized as csv processor
        if not hasattr(self, '_headers') or not hasattr(self, '_pcid'):
            raise Exception('Cipher are not initailized as a csv processor')

        hdrs = self._headers.copy()
        for c in self._pcid:
            hdrs.append(f'{self._headers[c]}_enc')
        hdrs.append('aws_enc_ctx')
        return hdrs

    def _process_csv_line(self, line):
        # jason.chia: not to be used directly
        # instead, refer to how to use in sanity test below

        _decode = False
        if isinstance(line, str):
            _decode = True
            line = line.encode(self.codec)

        _split_ori = line.split(self._delimiter)
        _split = [item.replace(b'\n', b'') for item in _split_ori]
        for c in self._pcid:
            tmp = _split[c]
            if c in self._shcid:
                _split[c] = self._slow_hash_field(tmp)
            else:
                _split[c] = self._hash_field(tmp)
            _split.append(self._encrypt_field(tmp))
        _split.append(self.aws_enc_ctx.encode(self.codec))

        out = self._delimiter.join(_split)
        if _decode:
            out = out.decode(self.codec)
        return out

    def detect_encoding(file):
        detector = chardet.universaldetector.UniversalDetector()
        with open(file, "rb") as f:
            for line in f:
                detector.feed(line)
                if detector.done:
                    break
        detector.close()
        return detector.result['encoding']

if __name__ == "__main__":
    # jason.chia: sanity test
    # PLEASE REFER HERE ON HOW TO USE THIS CLASS!

    # define headers (if the headers are on the file, you can use the first line to do this instead)
    hdrs = ['uuid', 'name', 'nric', 'date', 'cardnum']
    # initialize the cipher
    cip = Cipher('edb72148-81e8-43ed-96c0-abc9b7212126', 'salt123')
    # initialize the cipher as a csv processor
    cip.initialize_as_csv_processor(hdrs, ['name', 'nric', 'cardnum'], '|', slow_hashing=['cardnum'])

    # print the new headers
    print('|'.join(cip.new_headers))

    with open('cipher-test.csv', 'r') as fp:
        # read the test csv line by line
        for line in fp:
            # process the lines and print em
            print(cip.process_line(line))

    with open('cipher-test.csv', 'rb') as fp:
        # read the test csv line by line
        for line in fp:
            # process the lines and print em
            print(cip.process_line(line))
