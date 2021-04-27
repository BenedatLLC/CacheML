"""Support for encrypting/decrypting files.
"""


import binascii
from contextlib import contextmanager

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

BUF_SIZE=1024*1024

def bin_to_hex_str(bdata):
    return binascii.hexlify(bdata).decode('ascii')


def hex_str_to_bin(hdata):
    return binascii.unhexlify(hdata.encode('ascii'))


def get_new_key():
    return bin_to_hex_str(get_random_bytes(16))


def _get_nonce(filepath):
    """joblib writes a file with a thread id an then later renames it. Thus,
    the nonce is going to be wrong if we use the file name as-is. Instead,
    we look for the .pkl or .json extension and get the 16 characters before that.

    Although the max size of the nonce is 15 bytes, we use the recommended size of 8,
    as the larger the noncee the smaller the max message size. See the following
    stack overflow post for details:
    https://stackoverflow.com/questions/43462061/pycryptodome-overflowerror-the-counter-has-wrapped-around-in-ctr-mode
    """
    if '.pkl' in filepath:
        base = filepath[0:filepath.rindex('.pkl')]
    elif '.json' in filepath:
        base = filepath[0:filepath.rindex('.json')]
    elif '.py' in filepath:
        base = filepath[0:filepath.rindex('.py')]
    else:
        assert 0, f"unexpected filetype {filepath}"

    encoded = base.encode('utf-8')
    if len(encoded)<=8:
        return encoded
    else:
        return encoded[-8:]


class EncryptedFile:
    def __init__(self, filename, mode, key, buf_size):
        self.filename = filename
        self.key = key
        self.mode = mode
        self.buf_size = buf_size
        self.nonce = _get_nonce(filename)
        self._reset_crypto()
        self.fileobj = open(filename, mode, buffering=0)

    def _reset_crypto(self):
        self.cipher = AES.new(hex_str_to_bin(self.key), AES.MODE_CTR, nonce=self.nonce)

    def close(self):
        self.fileobj.close()

class FindableView:
    """A class like a memoryview on top of a bytes instance. It is missing most
    of the memoryview functionality, but provides a find() method, which is needed
    to implement readline().

    start should point to the start of the readable content, and end to one past the
    end of the readable content (e.g. as in slicing)"""
    __slots__=('buf', 'start', 'end')
    def __init__(self, buf, start, end):
        assert end>=start, f"Start was {start}, end was {end}"
        assert end<=len(buf)
        self.buf = buf
        self.start = start
        self.end=end
        #print(f"Findableview(start={self.start}, end={self.end})")

    def __len__(self):
        return self.end-self.start

    def find(self, sub):
        """Find returns an index relative to start, not relative to the start of the buffer.
        """
        i =  self.buf.find(sub, self.start, self.end)
        if i==(-1):
            return i
        else:
            return i - self.start

    def split(self, first_length):
        """Split this buffer into two parts, where the first part is first_length long and
        the rest is the remainder of the buffer. The first part is returned as a copy.
        The buffer is then advanced to point to the rest (it may be zero length).
        """
        assert first_length<=len(self)
        first = bytes(self.buf[self.start:self.start+first_length])
        self.start=self.start+first_length
        return first

    def tobytes(self):
        """Makes a copy of the buffer part that is in range"""
        return bytes(self.buf[self.start:self.end])


class EncryptedReader(EncryptedFile):
    def __init__(self, filename, mode, key, buf_size):
        assert mode.startswith('r')
        super().__init__(filename, mode, key, buf_size)
        self.ciphertext = bytearray(buf_size)
        self.cipherview = memoryview(self.ciphertext)
        self.cleartext = bytearray(buf_size)
        self.clearview = memoryview(self.cleartext)
        # If there is extra in the buffer at the end of a call,
        # we track it with a subview of clearview. This needs to be
        # checked in the next call.
        self.clearextra = None

    def read(self, size=(-1)):
        # if self.clearextra is not None:
        #     print(f"call to read: read(size={size}), clear_extra={len(self.clearextra)} bytes")
        # else:
        #     print(f"call to read: read(size={size}), clear_extra=None")

        # Parts is used when we need to combine the results from multiple
        # reads. The elements should always be a copied bytearray (vs. a view
        # into the cleartext buffer), unless it is the last element of multiple
        # and no more will be added.
        parts = []
       # print(f"file position before read: {self.fileobj.tell()}")
        if size>0 and (self.clearextra is not None) and size<=len(self.clearextra):
            # we can satisfy the request out of the left over from the last call
            result = self.clearextra.split(size)
            if len(self.clearextra)==0:
                self.clearextra = None
            return result
        elif self.clearextra is not None:
            parts.append(self.clearextra.tobytes())
            bytes_ready = len(self.clearextra)
            self.clearextra = None
        else:
            bytes_ready = 0
        if size==(-1):
            # Read the entire file as a single batch.
            # No point using our buffers, as we don't know the expect
            # size and the decrypted data must be returned as a copy.
            ciphercontents = self.fileobj.read(-1)
            parts.append(self.cipher.decrypt(ciphercontents))
            if len(parts)>1:
                return b''.join(parts)
            else:
                return parts[0]
        # if we get here, we are reading into the buffers
        #print(f"entering loop, bytes_ready={bytes_ready}")
        while True:
            bytes_read = self.fileobj.readinto(self.ciphertext)
            #print(f"read {bytes_read} bytes of ciphertext (buf_size was {self.buf_size})")
            if bytes_read==0: # got to end of file
                if len(parts)>1:
                    #print(f"read consists of {len(parts)} parts")
                    return b''.join(parts)
                elif len(parts)==1:
                    #print(f"return orphan part of length {len(parts[0])}")
                    return parts[0]
                else:
                    return bytes()
            # based on the api, the decrypted bytes always
            # equals the size of the cipher text
            decrypted_view = self.clearview[0:bytes_read]
            self.cipher.decrypt(self.cipherview[0:bytes_read],
                                output=decrypted_view)
            bytes_ready += bytes_read
            #print(f"bytes_read = {bytes_read}, bytes_ready now is {bytes_ready}")
            if bytes_ready==size:
                parts.append(decrypted_view)
                if len(parts)>1:
                    return b''.join(parts)
                else:
                    return decrypted_view.tobytes()
            elif bytes_ready>size:
                extra_bytes = bytes_ready-size
                bytes_to_return = bytes_read - extra_bytes
                parts.append(self.clearview[0:bytes_to_return])
                self.clearextra = FindableView(self.cleartext, bytes_to_return, bytes_read)
                if len(parts)>1:
                    r =  b''.join(parts)
                    #print(f"returning {len(r)} bytes")
                    return r
                else:
                    return parts[0].tobytes()
            else:
                assert bytes_ready<size
                parts.append(bytes(self.cleartext[0:bytes_read]))
                assert self.clearextra is None
            # need to do another read

    def readline(self, size=(-1)):
        assert size==(-1), f"currently do not suport readline with a size (size was {size})"
        # if self.clearextra is not None:
        #     print(f"call to readline(): clear_extra={len(self.clearextra)} bytes")
        # else:
        #     print("call to readline(): clear_extra=None")

        # Parts is used when we need to combine the results from multiple
        # reads. The elements should always be a copied bytearray (vs. a view
        # into the cleartext buffer), unless it is the last element of multiple
        # and no more will be added.
        parts = []

        if self.clearextra is not None:
            i = self.clearextra.find(b'\n')
            if i>=0:
                # we can do this entirely from the cleartext
                result = self.clearextra.split(i+1)
                if len(self.clearextra)==0:
                    self.clearextra = None
                #print(f"readline(): was able to get line from clearextra, len was {len(result)}")
                return result
            else:
                # the remainder from last time is not enough, save it to parts to free up buffer
                parts.append(self.clearextra.tobytes())
                self.clearextra = None
        while True:
            bytes_read = self.fileobj.readinto(self.ciphertext)
            #print(f"read {bytes_read} bytes of ciphertext (buf_size was {self.buf_size})")
            if bytes_read==0:
                if len(parts)>1:
                    #print(f"line consists of {len(parts)} parts")
                    return b''.join(parts)
                elif len(parts)==1:
                    #print(f"return orphan part of length {parts[0]}")
                    return parts[0]
                else:
                    return bytes()
            decrypted_view = self.clearview[0:bytes_read]
            self.cipher.decrypt(self.cipherview[0:bytes_read],
                                output=decrypted_view)
            i = self.cleartext.find(b'\n', 0, bytes_read)
            if i<0: # no newline found
                slice = self.cleartext[0:bytes_read] # will make a copy
                parts.append(slice)
            else:
                slice = self.cleartext[0:i+1]
                if bytes_read>(i+1):
                    self.clearextra = FindableView(self.cleartext, i+1, bytes_read)
                else:
                    self.clearextra = None
                if len(parts)>0:
                    parts.append(slice)
                    line =b''.join(parts)
                    #print(f"line consists of {len(parts)} parts")
                    return line
                else:
                    return slice

    def seek(self, offset, whence=0):
        """We only support resetting the postition base to the start of file.
        In that case, we also have to reset the decryption cipher state.
        """
        assert offset==0 and whence==0, f"Unexpected parameters for seek: (offset={offset}, whence={whence})"
        print(f"seek({offset}, {whence})")
        self._reset_crypto()
        self.clearextra = None
        return self.fileobj.seek(offset, whence)

    def close(self):
        super().close()
        # release buffers asap
        self.cipherview = self.clearview = self.clearextra = None
        self.ciphertext = self.cleartext = None




class EncryptedWriter(EncryptedFile):
    """We buffer the data as cleartext and then translate a buffer at a time
    as we write the buffers out"""
    def __init__(self, filename, mode, key, buf_size):
        assert mode.startswith('w')
        super().__init__(filename, mode, key, buf_size=buf_size)
        self.cleartext = bytearray(buf_size)
        self.clearview = memoryview(self.cleartext)
        self.bytes_in_buf = 0 # bytes already copied to the cleartext buffer
        self.ciphertext = bytearray(buf_size) # buffer to use in translations
        #print(f"Writer buf_size={buf_size}")


    def write(self, b):
        #print(f"write() ({len(b)} bytes), bytes_written={self.bytes_in_buf}")
        writeview = memoryview(b)
        bytes_to_write = len(b)
        while bytes_to_write>0:
            room_in_buf = self.buf_size - self.bytes_in_buf
            assert room_in_buf>0
            this_group_size = min(room_in_buf, bytes_to_write)
            #print(f"  room_in_buf = {room_in_buf}, this_group_size = {this_group_size}")
            end_idx = self.bytes_in_buf + this_group_size
            source_offset = len(b) - bytes_to_write
            self.clearview[self.bytes_in_buf:end_idx] = writeview[source_offset:source_offset+this_group_size]
            self.bytes_in_buf += this_group_size
            bytes_to_write -= this_group_size
            if end_idx==self.buf_size: # exactly buf size, empty buffer
                self.cipher.encrypt(self.cleartext, output=self.ciphertext)
                self.fileobj.write(self.ciphertext)
                self.bytes_in_buf = 0
                #print(f"wrote {len(self.cleartext)} bytes")
        return len(b)

    def flush(self):
        if self.bytes_in_buf>0:
            cipherview = memoryview(self.ciphertext)[0:self.bytes_in_buf]
            self.cipher.encrypt(self.clearview[0:self.bytes_in_buf], cipherview)
            self.fileobj.write(cipherview)
            self.bytes_in_buf = 0

    def close(self):
        self.flush()
        super().close()
        self.cleartext = None

class EncryptedWriterNoClearBuf(EncryptedFile):
    """Encrypted writer with no buffering of cleartext - it immediately encrypts the
    write data and stores the encrypted text in a buffer.
    """
    def __init__(self, filename, mode, key, buf_size):
        assert mode.startswith('w')
        super().__init__(filename, mode, key, buf_size=buf_size)
        self.ciphertext = bytearray(buf_size)
        self.cipherview = memoryview(self.ciphertext)
        self.bytes_written = 0 # bytes written to the ciphertext buffer
        #print(f"Writer buf_size={buf_size}")


    def write(self, b):
        #print(f"write() ({len(b)} bytes), bytes_written={self.bytes_written}")
        writeview = memoryview(b)
        bytes_to_write = len(b)
        while bytes_to_write>0:
            room_in_buf = self.buf_size - self.bytes_written
            assert room_in_buf>0
            this_group_size = min(room_in_buf, bytes_to_write)
            #print(f"  room_in_buf = {room_in_buf}, this_group_size = {this_group_size}")
            end_idx = self.bytes_written + this_group_size
            source_offset = len(b) - bytes_to_write
            this_write_buf = writeview[source_offset:source_offset+this_group_size]
            cipherview_subbuf = self.cipherview[self.bytes_written:end_idx]
            #print(f"[{self.bytes_written}:{end_idx}] cleartext len={len(this_write_buf)}, cipher len={len(cipherview_subbuf)}")
            self.cipher.encrypt(this_write_buf, output=cipherview_subbuf)
            self.bytes_written += this_group_size
            bytes_to_write -= this_group_size
            if end_idx==self.buf_size: # exactly buf size, empty buffer
                self.fileobj.write(self.ciphertext)
                self.bytes_written = 0
                #print(f"wrote {len(self.ciphertext)} bytes")
        return len(b)

    def flush(self):
        if self.bytes_written>0:
            self.fileobj.write(self.ciphertext[0:self.bytes_written])
            #print(f"Flushed {self.bytes_written} bytes")
            self.bytes_written = 0

    def close(self):
        if self.bytes_written>0:
            self.fileobj.write(self.ciphertext[0:self.bytes_written])
            #print(f"Flushed {self.bytes_written} bytes")
            self.bytes_written = 0
        super().close()
        self.ciphertext = None


@contextmanager
def encrypted_file_open(filename, mode, key, buf_size=BUF_SIZE):
    #print(f"encrypted_file_open({filename}, {mode})")
    if mode.startswith('r'):
        fileobj = EncryptedReader(filename, mode, key, buf_size=buf_size)
    elif mode.startswith('w'):
        fileobj = EncryptedWriterNoClearBuf(filename, mode, key, buf_size=buf_size)
    else:
        assert 0, f"Invalid mode '{mode}'"
    try:
        yield fileobj
    finally:
        fileobj.close()