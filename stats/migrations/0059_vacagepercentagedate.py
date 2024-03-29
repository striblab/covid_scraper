# Generated by Django 3.1.3 on 2021-04-13 20:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0058_auto_20210306_1107'),
    ]

    operations = [
        migrations.CreateModel(
            name='VacAgePercentageDate',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('county', models.CharField(db_index=True, max_length=50)),
                ('released_date', models.DateField(db_index=True)),
                ('asof_date', models.DateField(db_index=True, null=True)),
                ('age_group', models.CharField(db_index=True, max_length=10)),
                ('pct_one_dose_plus', models.FloatField(null=True)),
                ('pct_completed_series', models.FloatField(null=True)),
                ('update_date', models.DateTimeField(auto_now=True)),
            ],
        ),
    ]
