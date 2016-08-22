#!/usr/bin/env python

import shutil
import logging
import os.path
import json
from fnmatch import fnmatch

import click

logger = logging.getLogger(__name__)


def get_digest_from_blob(path):
    try:
        with open(path, 'rb') as blob:
            return blob.read().strip().split(':')[1]
    except Exception as e:
        logger.critical('Failed to read digest from blob: %s', e)


def get_links(path, filter=None):
    for root, _, files in os.walk(path):
        for each in files:
            if each == 'link':
                filepath = os.path.join(root, each)
                if not filter or ('%s%s%s' % (os.sep, filter, os.sep)) in filepath:
                    yield get_digest_from_blob(filepath)


def get_layers_from_blob(path):
    try:
        with open(path, 'rb') as blob:
            data = json.load(blob)
        if data['schemaVersion'] == 1:
            for entry in data['fsLayers']:
                yield entry['blobSum'].split(':')[1]
        elif data['schemaVersion'] == 2:
            for entry in data['layers']:
                yield entry['digest'].split(':')[1]
            if 'config' in data:
                yield data['config']['digest'].split(':')[1]
    except Exception as e:
        logger.critical('Failed to read layers from blob: %s', e)


class CleanerError(Exception):
    pass


class Cleaner(object):
    def __init__(self, data_dir, dry_run=True):
        self.data_dir = data_dir
        self.dry_run = dry_run

    def rmrf(self, path):
        if self.dry_run:
            logger.info('DRYRUN: would have deleted %s', path)
        else:
            logger.info('Deleting %s', path)
            try:
                shutil.rmtree(path)
            except Exception as e:
                logger.critical('Failed to delete directory: %s', e)

    def iter_links(self, exclude=None):
        "Iterate all links within a repository"
        for repo in (r for r in self.iter_repositories() if exclude is None or r != exclude):
            print(repo)
            path = os.path.join(self.data_dir, 'repositories', repo)
            for link in get_links(path):
                yield link

    def taginfo(self, repo, tag):
        "Get stat(1) info about a tag."
        return os.stat(
            os.path.join(
                self.data_dir, 'repositories', repo, '_manifests', 'tags', tag, 'current', 'link',
            )
        )

    def iter_tags(self, repo, exclude=''):
        "Iterate all tags within a repository."
        path = os.path.join(self.data_dir, 'repositories', repo, '_manifests', 'tags')
        if not os.path.isdir(path):
            logger.critical("No repository '%s' found in repositories directory %s",
                            repo, self.data_dir)
            return
        exclude = exclude.split(',')
        for each in os.listdir(path):
            filepath = os.path.join(path, each)
            if os.path.isdir(filepath):
                for ex in exclude:
                    if fnmatch(each, ex):
                        break
                else:
                    yield each, self.taginfo(repo, each)

    def tags(self, repo, exclude=''):
        return list(self.iter_tags(repo, exclude))

    def iter_repositories(self):
        "Iterate over all repositories in registry."
        root = os.path.join(self.data_dir, 'repositories')
        for each in os.listdir(root):
            filepath = os.path.join(root, each)
            if os.path.isdir(filepath):
                yield each

    def _blob_path_for_revision(self, digest):
        return os.path.join(self.data_dir, 'blobs', 'sha256', digest[:2], digest, 'data')

    def layers_from_blob(self, digest):
        return get_layers_from_blob(self._blob_path_for_revision(digest))

    def _manifest_in_same_repo(self, repo, tag, manifest):
        """check if manifest is found in other tags of same repository"""
        for other_tag in (t for t, _ in self.iter_tags(repo) if t != tag):
            path = os.path.join(self.data_dir, 'repositories', repo,
                                '_manifests', 'tags', other_tag, 'current', 'link')
            other_manifest = get_digest_from_blob(path)
            if other_manifest == manifest:
                return True
        return False

    def _layer_in_same_repo(self, repo, tag, layer):
        for other_tag in (t for t, _ in self.iter_tags(repo) if t != tag):
            path = os.path.join(self.data_dir, 'repositories', repo, '_manifests', 'tags', other_tag, 'current', 'link')
            for l in self.layers_from_blob(get_digest_from_blob(path)):
                if l == layer:
                    return True
        return False

    def _delete_from_tag_index_for_revision(self, repo, digest):
        tags_dir = os.path.join(self.data_dir, 'repositories', repo, '_manifests', 'tags')
        for tag in os.listdir(tags_dir):
            self.rmrf(os.path.join(tags_dir, tag, 'index', 'sha256', digest))

    def delete_layer(self, repo, digest):
        path = os.path.join(self.data_dir, 'repositories', repo, '_layers', 'sha256', digest)
        self.rmrf(path)

    def delete_revision(self, repo, revision):
        path = os.path.join(self.data_dir, 'repositories', repo, '_manifests', 'revisions', 'sha256', revision)
        for digest in set(get_links(path)):
            self._delete_from_tag_index_for_revision(repo, digest)
        self.rmrf(path)

    def delete_tag(self, repo, tag):
        logger.debug('Deleting %s:%s' % (repo, tag))
        tag_dir = os.path.join(self.data_dir, 'repositories', repo, '_manifests', 'tags', tag)
        if not os.path.isdir(tag_dir):
            raise CleanerError('No image %s:%s found' % (repo, tag))

        manifests_for_tag = set(get_links(tag_dir))
        revisions_to_delete = set()
        layers = set()

        for manifest in manifests_for_tag:
            logger.debug('Looking up filesystem layers for manifest digest %s', manifest)

            if self._manifest_in_same_repo(repo, tag, manifest):
                logger.debug('Not deleting since we found another tag using manifest: %s', manifest)
                continue

            revisions_to_delete.add(manifest)
            layers |= set(self.layers_from_blob(manifest))

        for layer in layers:
            if self._layer_in_same_repo(repo, tag, layer):
                logger.debug('Not deleting since we found another tag using digest: %s', layer)
                continue
            self.delete_layer(repo, layer)

        for revision in revisions_to_delete:
            self.delete_revision(repo, revision)

        self.rmrf(tag_dir)

    def delete_untagged(self, repo):
        repositories_dir = os.path.join(self.data_dir, 'repositories')
        repo_dir = os.path.join(repositories_dir, repo)
        if not os.path.isdir(repo_dir):
            raise CleanerError('Repository not found: %s' % repo)
        tagged_links = set(get_links(repositories_dir, filter='current'))
        layers_to_protect = set()
        for link in tagged_links:
            layers_to_protect |= set(self.layers_from_blob(link))

        tagged_revisions = set(get_links(repo_dir, filter='current'))
        revisions_to_delete = set()
        layers_to_delete = set()

        dir_for_revisions = os.path.join(repo_dir, '_manifests', 'revisions', 'sha256')
        for rev in os.listdir(dir_for_revisions):
            if rev not in tagged_revisions:
                revisions_to_delete.add(rev)
                for layer in self.layers_from_blob(rev):
                    if layer not in layers_to_protect:
                        layers_to_delete.add(layer)

        if not (revisions_to_delete or layers_to_delete):
            return

        logger.debug('Deleting untagged data from repository %r', repo)
        for revision in revisions_to_delete:
            self.delete_revision(repo, revision)

        for layer in layers_to_delete:
            self.delete_layer(repo, layer)


@click.command()
@click.option(
    '--data-dir',
    default='/var/lib/registry/docker/registry/v2',
    type=click.Path(exists=True),
)
@click.option('--repository', required=True)
@click.option('--n', default=30, type=click.IntRange(min=0))
@click.option('--dry-run/-d', default=False, is_flag=True)
@click.option('--exclude', default='')
def main(data_dir, n, repository, dry_run, exclude):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(u'%(levelname)-8s [%(asctime)s]  %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    c = Cleaner(data_dir, dry_run)
    tags = c.tags(repository, exclude=exclude)
    if len(tags) > n:
        tags = sorted(tags, key=lambda k: k[1].st_mtime, reverse=True)
        for tag, _ in tags[n:]:
            c.delete_tag(repository, tag)
    c.delete_untagged(repository)


if __name__ == '__main__':
    main()
