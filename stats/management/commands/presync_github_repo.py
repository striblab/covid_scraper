import os
from git import Repo
from datetime import datetime

from django.conf import settings

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Do a git pull before you start scraping so you don't have to squash commits later."

    REPO_LOCAL_PATH = os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data')

    def handle(self, *args, **options):
        repo = Repo(self.REPO_LOCAL_PATH)
        git = repo.git

        ssh_cmd = 'ssh -i /srv/keys/.docker_deploy_rsa'
        with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_cmd):
            git.stash()
            git.pull()
