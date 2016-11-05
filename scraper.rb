require 'scraperwiki'
require 'geokit'
require 'spreadsheet'
require 'pry'

# Set an API key if provided
Geokit::Geocoders::GoogleGeocoder.api_key = ENV['MORPH_GOOGLE_API_KEY'] if ENV['MORPH_GOOGLE_API_KEY']

@mappings = {
  1  => 'food_business_operator',
  2  => 'trading_name',
  3  => 'defendant',
  4  => 'address',
  5  => 'postown',
  6  => 'county',
  7  => 'postcode',
  8  => 'offence_category',
  9  => 'offence_provision',
  10 => 'contravention_in_eu_regulations',
  11 => 'nature_of_offence',
  12 => 'date_of_conviction',
  13 => 'conviction_or_guilty_plea',
  14 => 'court_name',
  15 => 'region',
  16 => 'sentence',
  17 => 'costs_awarded',
  18 => 'prosecution_authority'
}
class String
  def to_md5
    Digest::MD5.new.hexdigest(self)
  end
end

def scrub_date(value)
  case
  when value.class == DateTime
    return value.to_date
  when value.class == String
    return Date.parse(value)
  when value.class == Date
    return value
  else
    puts "[debug] Unhandled date: #{value.inspect}"
  end
end

def generate_id(details)
  return details.values.map(&:to_s).join(' ').to_md5
end

def build_prosecution(row)
  details = {}
  row.each_with_index do |value, index|
    key = @mappings[index]
    case key
    when nil
      next
    when 'date_of_conviction'
      value = scrub_date(value)
    else
      # Remove all leading and trailing whitespace
      value.strip if value.is_a? String
    end
    details.merge!({key => value})
  end
  details['id'] = generate_id(details)
  return details
end

def geocode(prosecution)
  @addresses ||= {}

  address = prosecution['address']
  address = [
    prosecution['address'],
    prosecution['county'],
    prosecution['postcode'],
  ].join(', ')

  if @addresses[address]
    puts "Geocoding [cache hit] #{address}"
    location = @addresses[address]
  else
    puts "Geocoding #{address}"
    a = Geokit::Geocoders::GoogleGeocoder.geocode(address)
    location = {
      'lat' => a.lat,
      'lng' => a.lng,
    }

    @addresses[address] = location
  end

  prosecution.merge!(location)
end

def existing_record_ids
  return @cached if @cached
  @cached = ScraperWiki.select('id from data').map {|r| r['id']}
rescue SqliteMagic::NoSuchTable
  []
end

def fetch_prosecutions
  url = "https://www.food.gov.uk/sites/default/files/prosecution-outcomes.xls"
  xls = open(url)

  sheet = Spreadsheet.open(xls).worksheet(0)
  header_row = sheet.row(5)
  max = sheet.rows.size - 1

  sheet.rows[6..max]
end

def main
  prosecutions = fetch_prosecutions
  prosecutions.map! { |p| build_prosecution(p) }

  puts "### Found #{prosecutions.size} notices"
  new_prosecutions = prosecutions.select {|r| !existing_record_ids.include?(r['id'])}
  puts "### There are #{new_prosecutions.size} new prosecutions"
  new_prosecutions.map! {|p| geocode(p) }

  # Serialise
  ScraperWiki.save_sqlite(['id'], new_prosecutions)

  puts "Done"
end

main()
