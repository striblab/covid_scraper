import os
from git import Repo
from datetime import datetime

from django.conf import settings

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Do a git pull before you start scraping so you don't have to squash commits later."

    REPO_LOCAL_PATH = os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data')

    # def clone_repo():
    #     '''You need to clone with git:// but you can't commit with keys that way, so we need to set a separate push url'''
    #     repo = Repo.clone_from('git://github.com/striblab/mn_covid_data.git', '/tmp/mn_covid_data', env=commit_env)
    #     with repo.remotes.origin.config_writer as cw:
    #         cw.set("pushurl", "git@github.com:striblab/mn_covid_data.git")
    #     return repo

    def handle(self, *args, **options):
        # repo = Repo(self.REPO_LOCAL_PATH)
        # git = repo.git
        repo = Repo.clone_from('git://github.com/striblab/mn_covid_data.git', self.REPO_LOCAL_PATH)
        ssh_cmd = 'ssh -i /srv/keys/.docker_deploy_rsa'
        with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_cmd):
            # git.pull('origin', 'master', '--rebase')
            with repo.remotes.origin.config_writer as cw:
                cw.set("pushurl", "git@github.com:striblab/mn_covid_data.git")
