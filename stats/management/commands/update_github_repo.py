import os
from git import Repo

from django.conf import settings

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Dump a CSV of the latest cumulative count of county-by-county positive tests.'

    REPO_LOCAL_PATH = os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data')

    def handle(self, *args, **options):
        repo = Repo(self.REPO_LOCAL_PATH)
        git = repo.git

        ssh_cmd = 'ssh -i /srv/keys/.docker_deploy_rsa'
        with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_cmd):
            git.add('mn_*.csv')

            if repo.is_dirty():
                print('Changes found, committing and pushing.')
                git.commit('-am', 'Updating scraper-generated files...')
                git.push('origin', 'master')
