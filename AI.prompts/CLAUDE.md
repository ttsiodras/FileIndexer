I want you to write a Python script that takes as input a folder name. It will
then recursively scan this folder for files, and populate a sqlite3 database
with a table called "files" that has columns (filename, full_path, top_folder,
mtime, md5, filesize). If it sees a file that is not already in the table, it
must insert the row - computing the md5 checksum. If the table has rows that no
longer correspond to files, I want the row to be removed. Finally, if the
filesize or mtime have been changed, I want you to update the row - recomputing
the md5 checksum. While it runs, I want the script to be able to use all available cores
in the machine to compute the MD5 checksums. The actual number will be selected
with a command line option. Finally, I want the scan that finds differences to
happen at the beginning, collecting all data from the table,
then all from the filesystem, then after the desired actions have been deduced
(insert new, update, delete) then the actions must be done - using as I said
all cores for the MD5 checksums.

In addition to accepting a top_folder as an argument, I want you to add the following options:

"-n/--ncores" controls how many will be used to compute the MD5s in parallel
(defaults to all cores available).

"-l,--limit <limit>" checks that there are at least <limit> copies of each
unique (filepath,MD5) across different top_folders (think of top_folders as
mounted external USB drives; so we want duplication for safety). 

"-v/--validate" will check every single row in the table and generate a
report.log with files that match their md5s, files that mismatch their md5s,
files that are missing, and new files that are not in the DB. If an argument is
passed, it's the top_folder to validate records for. If 'all' is passed, then
all rows will be validated.

Both the -l/--limit and the -v/--validate options should
identify-missing-copies and validate based on full_path+md5, not just md5. e.g.
if a file with an md5sum of X exists in both folder /iso3/a and folder /iso3/b,
the -l/--limit check would be "happy" because it found it twice under the
top_folder "/iso3" - but that is not the intent. The intent is to check that
(/a,X) does not appear less than "limit" times (e.g. it has to exist as both
("/iso3","/a",X) and ("/iso5", "/a", X) to satisfy a -l 2. The validation by
definition exercises the fullpath to compare against the stored md5; and by the
way, make sure it does respect the -n option; that is, both when creating and
when valuidating the checksums, we want to control how many parallel executors
are being used.

Note that the script must be tolerant of completely broken filenames,
so the sqlite3 table must use blob to store the filename and full_path.
It can convert it to show only printable characters in the output,
but it must be able to cope with complete garbage characters in the filenames,
not just UTF-8 compliant ones.
