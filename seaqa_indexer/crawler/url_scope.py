import re
from urllib.parse import urlparse


class URLScope:
    """Restrict crawled URLs to the configured site path and exclusions."""

    def __init__(self, site_url, exclude_paths=None):
        parsed_site = urlparse(site_url)
        self.scheme = parsed_site.scheme.lower()
        self.netloc = parsed_site.netloc.lower()
        self.base_path = self._normalize_path(parsed_site.path)
        last_segment = self.base_path.rsplit('/', 1)[-1]
        self.is_file = (
            not parsed_site.path.endswith('/')
            and bool(re.search(r'\.[A-Za-z][A-Za-z0-9_-]*$', last_segment))
        )
        if isinstance(exclude_paths, str):
            exclude_paths = exclude_paths.splitlines()
        normalized_exclude_paths = []
        for path in exclude_paths or []:
            path = str(path).strip()
            if path:
                normalized_exclude_paths.append(self._normalize_path(path))
        self.exclude_paths = tuple(normalized_exclude_paths)

    @staticmethod
    def _normalize_path(path):
        parsed = urlparse(str(path).strip())
        path = parsed.path or '/'
        if not path.startswith('/'):
            path = '/' + path
        return path if path == '/' else path.rstrip('/')

    @classmethod
    def _path_matches(cls, path, base_path):
        path = cls._normalize_path(path)
        base_path = cls._normalize_path(base_path)
        return base_path == '/' or path == base_path or path.startswith(base_path + '/')

    def allows(self, url):
        parsed_url = urlparse(url)
        if parsed_url.scheme.lower() != self.scheme or parsed_url.netloc.lower() != self.netloc:
            return False

        path = self._normalize_path(parsed_url.path)
        if self.is_file and path != self.base_path:
            return False
        if not self.is_file and not self._path_matches(path, self.base_path):
            return False
        return not any(self._path_matches(path, excluded_path) for excluded_path in self.exclude_paths)
