require 'nokogiri'
require 'open-uri'
require 'sqlite3'
require 'logger'
require 'uri'

# Initialize the logger
logger = Logger.new(STDOUT)

# Define the URL of the page
url = 'https://gsbc.tas.gov.au/services-facilities/public-notices/'

# Step 1: Fetch the iframe content using open-uri
begin
  logger.info("Fetching content from: #{url}")
  url = open(url).read
  logger.info("Successfully fetched content.")
rescue => e
  logger.error("Failed to fetch content: #{e}")
  exit
end

# Step 2: Parse the iframe content using Nokogiri
doc = Nokogiri::HTML(url)

# Step 3: Initialize the SQLite database
db = SQLite3::Database.new "data.sqlite"

# Create the table if it doesn't exist
db.execute <<-SQL
  CREATE TABLE IF NOT EXISTS glamorgan (
    id INTEGER PRIMARY KEY,
    description TEXT,
    date_scraped TEXT,
    date_received TEXT,
    on_notice_to TEXT,
    address TEXT,
    council_reference TEXT,
    applicant TEXT,
    owner TEXT,
    stage_description TEXT,
    stage_status TEXT,
    document_description TEXT
  );
SQL

# Define variables for storing extracted data for each entry
address = ''  
description = ''
on_notice_to = ''
date_received = ''
council_reference = ''
applicant = ''
owner = ''
stage_description = ''
stage_status = ''
document_description = ''
date_scraped = ''

# Step 4: Extract the data from the table
table = doc.at_css('table')
rows = table.css('tbody tr')

rows.each_with_index do |row, index|
  # Extract the columns for each row
  columns = row.css('td')
  next if columns.empty?

  # Extract the text content of each column
  description = columns[0].text.strip
  address = columns[1].text.strip
  application_date = columns[2].text.strip
  on_notice_to = columns[3].text.strip
  document_description = columns[4].css('a').first['href'] rescue nil
  date_scraped = Date.today.to_s

  # Log the extracted data
  logger.info("Extracted Data: #{name}, #{address}, #{application_date}, #{closing_date}, #{pdf_link}")

  # Step 6: Ensure the entry does not already exist before inserting
  existing_entry = db.execute("SELECT * FROM glamorgan WHERE council_reference = ?", [council_reference])

  if existing_entry.empty? # Only insert if the entry doesn't already exist
    # Insert the data into the database
    db.execute("INSERT INTO glamorgan (description, address, date_received, on_notice_to, document_description, date_scraped) VALUES (?, ?, ?, ?, ?, ?)",
             [description, address, date_received, on_notice_to, document_description, date_scraped])
    logger.info("Data for application #{council_reference} saved to database.")
  else
    logger.info("Duplicate entry for application #{council_reference} found. Skipping insertion.")
  end
end

puts "Data has been successfully inserted into the database."
