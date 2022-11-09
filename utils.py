import os
import pathlib

import boto3


class HCP_S3():
    client = boto3.client('s3')

    @classmethod
    def list_buckets(cls):
        """ List all buckets. """
        return [bucket["Name"] for bucket in cls.client.list_buckets()["Buckets"]]

    @classmethod
    def head(cls, key, bucket='hcp-openaccess'):
        """ Check if object exists.

        Parameters
        ----------
        key : str
            Object key (prefix).
        bucket : str, optional
            Bucket, by default 'hcp-openaccess'.

        Returns
        -------
        bool
            True if object with key exists, otherwise False.
        """
        return True if cls.client.head_object(Bucket=bucket, Key=key) else False

    @classmethod
    def ls(cls, prefix='', bucket='hcp-openaccess', maxkeys=1000, delimiter=False, endslash=True):
        """ Lists objects under prefix.

        Parameters
        prefix : str 
            Object to list (path prefix).
        bucket : str, optional
            Bucket to use, by default 'hcp-openaccess'.
        maxkeys : int, optional
            Maximum keys to return (<= 1000), by default 1000.
        delimiter : bool, optional
            Whether to limit search depth, by default False.
        endslash : bool, optional
            Whether to add slash at end of prefix, by default True.

        Returns
        -------
        list
            List of objects.
        """
        # delimiter = not (delimiter and head(prefix, bucket)) # If delimiter set True by default 

        prefix_ = prefix + '/' if prefix and not prefix.endswith('/') and delimiter and endslash else prefix

        if delimiter:
            res = cls.client.list_objects(Bucket=bucket, Delimiter='/', Prefix=prefix_, MaxKeys=maxkeys)
        else:
            res = cls.client.list_objects(Bucket=bucket, Prefix=prefix_, MaxKeys=maxkeys)

        try:
            if delimiter:
                return [prefix['Prefix'] for prefix in res['CommonPrefixes']]
            else:
                return [item['Key'] for item in res['Contents']]
        except KeyError:
            print(f"Invalid prefix '{prefix_}' (if it is not, set `delimiter` to False)")
            return False

    @classmethod
    def get(cls, key, bucket='hcp-openaccess'):
        """ Gets object from the bucket.

        Parameters
        ----------
        key : str
            Object key (prefix).
        bucket : str, optional
            Bucket to use, by default 'hcp-openaccess'.

        Returns
        -------
        dict
            Object if it exists else None.
        """
        if not cls.ls(key, bucket, delimiter=False): return
        
        return cls.client.get_object(Bucket=bucket, Key=key)

    @classmethod
    def download(cls, prefix, local='data', trim=0, bucket='hcp-openaccess'):
        """ Downloads objects under prefix recursively.

        Parameters
        ----------
        prefix : str 
            Directory to download (path prefix).
        local : str, optional
            Top level folder for downloads, by default 'data'.
        trim : int, optional
            Starting index to slice from root of S3 path (including bucket) to get local path relative to `local`, by default 0.
        bucket : str, optional
            Bucket to use, by default 'hcp-openaccess'.
        """
        keys = []
        dirs = []
        next_token = ''
        base_kwargs = {
            'Bucket': bucket,
            'Prefix': prefix,
        }
        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})
            results = cls.client.list_objects_v2(**kwargs)
            contents = results.get('Contents')
            for i in contents:
                k = i.get('Key')
                if k[-1] != '/':
                    keys.append(k)
                else:
                    dirs.append(k)
            next_token = results.get('NextContinuationToken')
        for d in dirs:
            dest_pathname = os.path.join(local, bucket, d)
            print(dest_pathname)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
        for k in keys:
            dest_pathname = str(pathlib.Path(local, *pathlib.Path(bucket, k).parts[trim:]))
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            cls.client.download_file(bucket, k, dest_pathname)

if __name__ == '__main__':
    pass