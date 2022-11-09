import os
import boto3


class hcp_s3():
    def __init__(self):
        self.client = boto3.client('s3')

    def list_buckets(self):
        """ List all buckets. """
        return [bucket["Name"] for bucket in self.client.list_buckets()["Buckets"]]

    def head(self, key, bucket='hcp-openaccess'):
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
        return True if self.client.head_object(Bucket=bucket, Key=key) else False

    def ls(self, prefix='', bucket='hcp-openaccess', maxkeys=1300, delimiter=False, endslash=True):
        """ Lists objects under prefix.

        Parameters
        prefix : str 
            Object to list (path prefix).
        bucket : str, optional
            Bucket to use, by default 'hcp-openaccess'.
        maxkeys : int, optional
            Maximum keys to return, by default 1300.
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
            res = self.client.list_objects(Bucket=bucket, Delimiter='/', Prefix=prefix_, MaxKeys=maxkeys)
        else:
            res = self.client.list_objects(Bucket=bucket, Prefix=prefix_, MaxKeys=maxkeys)

        try:
            if delimiter:
                return [prefix['Prefix'] for prefix in res['CommonPrefixes']]
            else:
                return [item['Key'] for item in res['Contents']]
        except KeyError:
            print(f"Invalid prefix '{prefix_}' (if it is not, set `delimiter` to False)")
            return False

    def get(self, key, bucket='hcp-openaccess'):
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
        if not self.ls(key, bucket, delimiter=False): return
        
        return self.client.get_object(Bucket=bucket, Key=key)

    def download(self, prefix, local='data', bucket='hcp-openaccess'):
        """ Downloads objects under prefix recursively.

        Parameters
        ----------
        prefix : str 
            Directory to download (path prefix).
        local : str, optional
            Top level folder for downloads, by default 'data'.
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
            results = self.client.list_objects_v2(**kwargs)
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
            dest_pathname = os.path.join(local, bucket, k)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            self.client.download_file(bucket, k, dest_pathname)

if __name__ == '__main__':
    pass