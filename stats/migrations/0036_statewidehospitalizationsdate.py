# Generated by Django 3.0.9 on 2020-09-24 15:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0035_auto_20200820_1329'),
    ]

    operations = [
        migrations.CreateModel(
            name='StatewideHospitalizationsDate',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('reported_date', models.DateField(null=True)),
                ('new_hosp_admissions', models.IntegerField(default=0, null=True)),
                ('new_icu_admissions', models.IntegerField(default=0, null=True)),
                ('total_hospitalizations', models.IntegerField(default=0)),
                ('total_icu_admissions', models.IntegerField(default=0)),
                ('update_date', models.DateField(null=True)),
                ('scrape_date', models.DateField()),
            ],
        ),
    ]