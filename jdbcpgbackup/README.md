## backup command line sample
```
-m dump -h localhost -p 5432 -d {database} -s {old_schema} -f {zip_file_absolate_path}
```

## restore command line sample
```
-m restore -h localhost -p 5432 -d {database} -s {new_schema} -f {zip_file_absolate_path}
```