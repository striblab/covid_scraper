# Generated by Django 3.1.3 on 2021-01-04 22:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0042_statewidetotaldate_icu_total_daily_change'),
    ]

    operations = [
        migrations.CreateModel(
            name='VacAdminTotalDate',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('data_date', models.DateField()),
                ('admin_total', models.IntegerField(null=True)),
                ('admin_pfizer', models.IntegerField(null=True)),
                ('admin_moderna', models.IntegerField(null=True)),
                ('admin_unknown', models.IntegerField(null=True)),
                ('update_date', models.DateTimeField(auto_now=True)),
            ],
        ),
    ]
