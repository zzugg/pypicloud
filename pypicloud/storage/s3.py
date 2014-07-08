""" Store packages in S3 """
import os
import logging
import time
from contextlib import contextmanager
from hashlib import md5
from urllib import urlopen

from pyramid.httpexceptions import HTTPNotFound, HTTPFound
from pyramid.settings import asbool

import boto
import posixpath
from .base import IStorage
from boto.s3.key import Key
from boto.cloudfront import CloudFrontConnection
from boto.cloudfront.distribution import Distribution
from pypicloud.models import Package
from pypicloud.util import parse_filename, getdefaults


LOG = logging.getLogger(__name__)


class S3Storage(IStorage):

    """ Storage backend that uses S3 """
    test = False

    def __init__(self, request=None, bucket=None, expire_after=None,
                 buffer_time=None, bucket_prefix=None, prepend_hash=None,
                 use_cloudfront=False, cf_distribution=None, cf_keypair_id=None, cf_private_key=None, cf_url=None,
                 **kwargs):
        super(S3Storage, self).__init__(request, **kwargs)
        self.bucket = bucket
        self.expire_after = expire_after
        self.buffer_time = buffer_time
        self.bucket_prefix = bucket_prefix
        self.prepend_hash = prepend_hash
        self.use_cloudfront = use_cloudfront
        self.cf_distribution = cf_distribution
        self.cf_keypair_id = cf_keypair_id
        self.cf_private_key = cf_private_key
        self.cf_url = cf_url

    @classmethod
    def configure(cls, settings):
        kwargs = super(S3Storage, cls).configure(settings)
        kwargs['expire_after'] = int(getdefaults(
            settings, 'storage.expire_after', 'aws.expire_after', 60 * 60 *
            24))
        kwargs['buffer_time'] = int(getdefaults(
            settings, 'storage.buffer_time', 'aws.buffer_time', 600))
        kwargs['bucket_prefix'] = getdefaults(
            settings, 'storage.prefix', 'aws.prefix', '')
        kwargs['prepend_hash'] = asbool(getdefaults(
            settings, 'storage.prepend_hash', 'aws.prepend_hash', True))
        access_key = getdefaults(settings, 'storage.access_key',
                                 'aws.access_key', None)
        secret_key = getdefaults(settings, 'storage.secret_key',
                                 'aws.secret_key', None)

        s3conn = boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)
        aws_bucket = getdefaults(settings, 'storage.bucket', 'aws.bucket',
                                 None)
        if aws_bucket is None:
            raise ValueError("You must specify the 'storage.bucket'")
        bucket = s3conn.lookup(aws_bucket, validate=False)
        if bucket is None:
            location = getdefaults(settings, 'storage.region', 'aws.region',
                                   boto.s3.connection.Location.DEFAULT)
            bucket = s3conn.create_bucket(aws_bucket, location=location)
        kwargs['bucket'] = bucket

        kwargs['use_cloudfront'] = asbool(settings.get('storage.cloudfront', False))
        if kwargs['use_cloudfront']:
            dist_id = settings.get('storage.cloudfront_distribution_id', None)
            cfrontconn = CloudFrontConnection(access_key, secret_key)
            dist_config = cfrontconn.get_distribution_config(dist_id)
            dist_info = cfrontconn.get_distribution_info(dist_id)
            distribution = Distribution(connection=cfrontconn,
                                        config=dist_config,
                                        domain_name=dist_info.domain_name,
                                        id=dist_id)
            kwargs['cf_distribution'] = distribution
            kwargs['cf_keypair_id'] = settings.get('storage.cloudfront_keypair_id', None)
            kwargs['cf_private_key'] = settings.get('storage.cloudfront_private_key', None)
            kwargs['cf_url'] = settings.get('storage.cloudfront_url', None)

        return kwargs

    def get_path(self, package):
        """ Get the fully-qualified bucket path for a package """
        if 'path' not in package.data:
            filename = package.name + '/' + package.filename
            if self.prepend_hash:
                m = md5()
                m.update(package.filename)
                prefix = m.digest().encode('hex')[:4]
                filename = prefix + '/' + filename
            package.data['path'] = self.bucket_prefix + filename
        return package.data['path']

    def list(self, factory=Package):
        keys = self.bucket.list(self.bucket_prefix)
        for key in keys:
            # Moto doesn't send down metadata from bucket.list()
            if self.test:
                key = self.bucket.get_key(key.key)
            filename = posixpath.basename(key.key)
            name = key.get_metadata('name')
            version = key.get_metadata('version')

            # We used to not store metadata. This is for backwards
            # compatibility
            if name is None or version is None:
                try:
                    name, version = parse_filename(filename)
                except ValueError:
                    LOG.warning("S3 file %s has no package name", key.key)
                    continue

            last_modified = boto.utils.parse_ts(key.last_modified)

            pkg = factory(name, version, filename, last_modified, path=key.key)

            yield pkg

    def get_url(self, package):
        expire = package.data.get('expire', 0)
        changed = False
        if 'url' not in package.data or time.time() > expire:
            if self.use_cloudfront:
                expire_after = int(time.time()) + self.expire_after
                unsigned_url = os.path.join(self.cf_url, self.get_path(package))
                url = self.cf_distribution.create_signed_url(unsigned_url,
                                                             self.cf_keypair_id,
                                                             expire_time=expire_after,
                                                             private_key_file=self.cf_private_key)
            else:
                key = Key(self.bucket)
                key.key = self.get_path(package)
                expire_after = time.time() + self.expire_after
                url = key.generate_url(expire_after, expires_in_absolute=True)
            package.data['url'] = url
            expire = expire_after - self.buffer_time
            package.data['expire'] = expire
            changed = True

        return package.data['url'], changed

    def download_response(self, package):
        try:
            return HTTPFound(self.get_url(package)[0])
        except:
            return HTTPNotFound()

    def upload(self, package, data):
        key = Key(self.bucket)
        key.key = self.get_path(package)
        key.set_metadata('name', package.name)
        key.set_metadata('version', package.version)
        # S3 doesn't support uploading from a non-file stream, so we have to
        # read it into memory :(
        key.set_contents_from_string(data.read())

    def delete(self, package):
        path = self.get_path(package)
        key = Key(self.bucket)
        key.key = path
        key.delete()

    @contextmanager
    def open(self, package):
        url = self.get_url(package)[0]
        handle = urlopen(url)
        try:
            yield handle
        finally:
            handle.close()
