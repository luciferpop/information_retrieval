import glob
import os
import zipfile
import xml.etree.ElementTree as ET

count = 0
process = 0

# open zip files
for zip in glob.glob('../data/*.zip'):
    zipname = filter(str.isdigit, zip)
    print 'Parsing file: ' + zipname + '\t' + str(process/3.65) + '%'
    # Open zip file and parse
    with zipfile.ZipFile(zip) as z:
        for filename in z.namelist():
            try:
                cf = open('../o/part_' + str(count), 'a')
                # File is larger than 256MB, close current file and open a new one
                if os.path.getsize('../o/part_' + str(count)) > 2 * 127 * 1024 * 1024L:
                    cf.close()
                    count += 1
                    cf = open('../o/part_' + str(count))
            except:
                cf = open('../o/part_' + str(count), 'w')
            
            # Open one xml file and parse the content in a zip
            with z.open(filename) as xml:
                # Reading file from disk
                tree = ET.parse(xml)
                root = tree.getroot()

                # String to save XML contents should be the format of (filename tab content)
                newfile = filter(str.isdigit, filename)
                s = newfile + '\t'
                     
                # get news' title
                for t in root.iter('title'):
                    if t.text is not None:
                        s += t.text                 # O(n)
                     
                # get news' headline
                for h in root.iter('headline'):
                    if h.text is not None:
                        s += h.text
                 
                # get all paragraphs
                for p in root.iter('p'):
                    if p.text is not None:
                        s += p.text
                     
                # save the contents into a digit-named file 
                try:
                    cf.write(s.encode('utf-8') + '\n')
                except:
                    write_log = open('../fail_write_log.txt', 'a')
                    write_log.write(filename + '\n')
                    write_log.write(s.encode('utf-8') + '\n')
    process += 1
print 'Finished.'
# list all files in data folder
#for filename in glob.glob('../data/*.xml'):
    
     

