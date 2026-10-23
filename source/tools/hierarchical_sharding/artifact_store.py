"""Private, version-pinned, content-addressed benchmark artifacts.

SHA-256 identifies every complete file. Multipart transport additionally uses a
server-validated full-object CRC32; multipart ETags are never treated as hashes.
"""
import base64
import hashlib
import os
from pathlib import Path
import struct
import uuid
import zlib


def file_checksums(path):
    sha=hashlib.sha256();crc=0;size=0
    with Path(path).open('rb') as stream:
        while True:
            block=stream.read(8*1024*1024)
            if not block:break
            sha.update(block);crc=zlib.crc32(block,crc);size+=len(block)
    return dict(bytes=size,sha256=sha.hexdigest(),crc32=base64.b64encode(struct.pack('>I',crc&0xffffffff)).decode())


class ArtifactStore:
    def __init__(self,bucket,account,s3,sts,role):
        identity=sts.get_caller_identity()
        if identity['Account']!=account or not identity['Arn'].startswith('arn:aws:sts::'+account+':assumed-role/'+role+'/'):
            raise ValueError('artifact client does not use the expected sandbox instance role')
        self.bucket=bucket;self.account=account;self.s3=s3
        location=s3.get_bucket_location(Bucket=bucket,ExpectedBucketOwner=account).get('LocationConstraint')
        if location not in (None,'us-east-1'):raise ValueError('artifact bucket has another region')

    def parameters(self,**values):
        return dict(Bucket=self.bucket,ExpectedBucketOwner=self.account,**values)

    def upload(self,path,part_bytes=64*1024*1024):
        if part_bytes<5*1024*1024:raise ValueError('multipart parts must be at least five MiB')
        checks=file_checksums(path);key='sha256/'+checks['sha256'][:2]+'/'+checks['sha256'];metadata={'content-sha256':checks['sha256']}
        if checks['bytes']<=part_bytes:
            with Path(path).open('rb') as body:
                response=self.s3.put_object(**self.parameters(Key=key,Body=body,ServerSideEncryption='AES256',Metadata=metadata,ChecksumAlgorithm='SHA256',ChecksumSHA256=base64.b64encode(bytes.fromhex(checks['sha256'])).decode()))
            checksum_field='ChecksumSHA256';checksum_value=base64.b64encode(bytes.fromhex(checks['sha256'])).decode()
        else:
            upload=self.s3.create_multipart_upload(**self.parameters(Key=key,ServerSideEncryption='AES256',Metadata=metadata,ChecksumAlgorithm='CRC32',ChecksumType='FULL_OBJECT'))
            upload_id=upload['UploadId'];parts=[]
            try:
                with Path(path).open('rb') as stream:
                    number=1
                    while True:
                        block=stream.read(part_bytes)
                        if not block:break
                        crc=base64.b64encode(struct.pack('>I',zlib.crc32(block)&0xffffffff)).decode()
                        part=self.s3.upload_part(**self.parameters(Key=key,UploadId=upload_id,PartNumber=number,Body=block,ChecksumCRC32=crc))
                        if part.get('ChecksumCRC32')!=crc:raise ValueError('uploaded part checksum differs')
                        parts.append(dict(PartNumber=number,ETag=part['ETag'],ChecksumCRC32=crc));number+=1
                if file_checksums(path)!=checks:raise ValueError('artifact changes during upload')
                response=self.s3.complete_multipart_upload(**self.parameters(Key=key,UploadId=upload_id,MultipartUpload=dict(Parts=parts),ChecksumCRC32=checks['crc32'],ChecksumType='FULL_OBJECT'))
            except BaseException:
                self.s3.abort_multipart_upload(**self.parameters(Key=key,UploadId=upload_id));raise
            checksum_field='ChecksumCRC32';checksum_value=checks['crc32']
        version=response.get('VersionId')
        if not version or version=='null':raise ValueError('artifact is not version-pinned')
        head=self.s3.head_object(**self.parameters(Key=key,VersionId=version,ChecksumMode='ENABLED'))
        if head['ContentLength']!=checks['bytes'] or head.get('Metadata',{}).get('content-sha256')!=checks['sha256'] or head.get(checksum_field)!=checksum_value or head.get('ServerSideEncryption')!='AES256':
            raise ValueError('stored artifact checksum, size, or encryption differs')
        return dict(bucket=self.bucket,expected_bucket_owner=self.account,key=key,version_id=version,**checks,transport_checksum_field=checksum_field,transport_checksum_value=checksum_value,encryption='AES256')

    def restore(self,descriptor,destination):
        if descriptor['bucket']!=self.bucket or descriptor['expected_bucket_owner']!=self.account:
            raise ValueError('restore descriptor names another bucket owner')
        expected_key='sha256/'+descriptor['sha256'][:2]+'/'+descriptor['sha256']
        if descriptor['key']!=expected_key:raise ValueError('restore key is not content-addressed')
        destination=Path(destination)
        if destination.exists():raise FileExistsError('restore destination already exists')
        destination.parent.mkdir(parents=True,exist_ok=True);temporary=destination.with_name(destination.name+'.partial-'+uuid.uuid4().hex)
        response=self.s3.get_object(**self.parameters(Key=descriptor['key'],VersionId=descriptor['version_id'],ChecksumMode='ENABLED'))
        if response['ContentLength']!=descriptor['bytes'] or response.get('ServerSideEncryption')!='AES256':raise ValueError('restored object size or encryption differs')
        sha=hashlib.sha256();size=0
        try:
            with temporary.open('xb') as output:
                while True:
                    block=response['Body'].read(8*1024*1024)
                    if not block:break
                    size+=len(block)
                    if size>descriptor['bytes']:raise ValueError('restored artifact exceeds expected bytes')
                    sha.update(block);output.write(block)
                output.flush();os.fsync(output.fileno())
        finally:response['Body'].close()
        if size!=descriptor['bytes'] or sha.hexdigest()!=descriptor['sha256']:
            raise ValueError('restored artifact SHA-256 differs; private partial output is not published')
        os.link(temporary,destination);temporary.unlink()
        return dict(path=str(destination),bytes=size,sha256=sha.hexdigest(),version_id=descriptor['version_id'])
