use chroma_benchmark::datasets::wikipedia_splade::WikipediaSplade;
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::{test_arrow_blockfile_provider, BlockfileWriterOptions};
use chroma_index::fulltext::types::{DocumentMutation, FullTextIndexReader, FullTextIndexWriter};
use chroma_types::regex::{
    literal_expr::{LiteralExpr, NgramLiteralProvider},
    ChromaRegex,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tantivy::tokenizer::NgramTokenizer;

// All 256 queries from query.parquet transformed to regex alternation patterns
// These test the FTS regex candidate selection performance
const QUERIES: &[&str] = &[
    // Automobile Industry (16 queries)
    "Toyota Corolla|Corolla Prius|Prius hybrid|hybrid reliability|reliability Japan",
    "Tesla Model|Model 3|3 Model|Model S|S electric|electric autopilot",
    "Ford F-150|F-150 Mustang|Mustang American|American trucks|trucks muscle",
    "Volkswagen Golf|Golf Beetle|Beetle diesel|diesel scandal|scandal Germany",
    "BMW luxury|luxury performance|performance M|M series|series German|German engineering",
    "Mercedes-Benz S-Class|S-Class luxury|luxury safety|safety technology",
    "Honda Civic|Civic Accord|Accord reliability|reliability fuel|fuel economy",
    "Ferrari supercar|supercar Italian|Italian racing|racing Formula|Formula 1",
    "General Motors|Motors Chevrolet|Chevrolet GMC|GMC Cadillac|Cadillac bankruptcy",
    "Nissan Altima|Altima Leaf|Leaf electric|electric alliance|alliance Renault",
    "Hyundai Kia|Kia South|South Korean|Korean value|value warranty",
    "Porsche 911|911 sports|sports car|car German|German engineering",
    "Audi quattro|quattro all-wheel|all-wheel drive|drive luxury|luxury Volkswagen",
    "Lamborghini supercar|supercar Italian|Italian exotic|exotic V12",
    "electric vehicles|vehicles EV|EV charging|charging battery|battery range",
    "autonomous driving|driving self-driving|self-driving cars|cars AI|AI sensors",
    // Climate and Weather (16 queries)
    "climate change|change global|global warming|warming greenhouse|greenhouse gases",
    "hurricanes categories|categories Atlantic|Atlantic Pacific|Pacific typhoons",
    "tornadoes Tornado|Tornado Alley|Alley F5|F5 damage|damage scale",
    "El Nino|Nino La|La Nina|Nina Pacific|Pacific Ocean|Ocean weather|weather patterns",
    "Arctic ice|ice melting|melting polar|polar bears|bears ecosystem",
    "rainforest Amazon|Amazon deforestation|deforestation oxygen|oxygen carbon",
    "drought California|California water|water shortage|shortage agriculture",
    "flooding monsoon|monsoon season|season Bangladesh|Bangladesh Venice",
    "wildfires Australia|Australia California|California smoke|smoke prevention",
    "renewable energy|energy solar|solar wind|wind power|power hydroelectric",
    "Paris Agreement|Agreement climate|climate accord|accord carbon|carbon emissions",
    "weather forecasting|forecasting meteorology|meteorology satellites|satellites radar",
    "ocean acidification|acidification coral|coral bleaching|bleaching Great|Great Barrier",
    "permafrost melting|melting methane|methane release|release Siberia",
    "extreme weather|weather events|events heatwaves|heatwaves records",
    "carbon footprint|footprint reduction|reduction sustainability",
    // Countries and Nations (16 queries)
    "United States|States president|president Congress|Congress Constitution",
    "China Communist|Communist Party|Party economy|economy manufacturing",
    "India population|population Bollywood|Bollywood democracy|democracy castes",
    "Russia Putin|Putin Soviet|Soviet Union|Union natural|natural gas",
    "Japan Tokyo|Tokyo technology|technology anime|anime culture",
    "Germany Berlin|Berlin EU|EU economy|economy engineering",
    "United Kingdom|Kingdom Brexit|Brexit monarchy|monarchy Parliament|Parliament London",
    "France Paris|Paris EU|EU wine|wine fashion|fashion culture",
    "Brazil Amazon|Amazon rainforest|rainforest Portuguese|Portuguese carnival",
    "Canada provinces|provinces healthcare|healthcare maple|maple syrup|syrup hockey",
    "Australia Sydney|Sydney Melbourne|Melbourne Great|Great Barrier|Barrier Reef",
    "South Korea|Korea Seoul|Seoul K-pop|K-pop technology|technology Samsung",
    "Italy Rome|Rome Vatican|Vatican pasta|pasta Renaissance|Renaissance art",
    "Mexico Spanish|Spanish Aztec|Aztec Maya|Maya tequila|tequila cartels",
    "Saudi Arabia|Arabia oil|oil Mecca|Mecca Islam|Islam monarchy",
    "South Africa|Africa apartheid|apartheid Mandela|Mandela gold|gold diamonds",
    // Food and Cuisine (16 queries)
    "Italian pizza|pizza pasta|pasta Rome|Rome Naples|Naples cuisine",
    "Chinese cuisine|cuisine Sichuan|Sichuan dim|dim sum|sum wok|wok stir-fry",
    "French cuisine|cuisine michelin|michelin stars|stars wine|wine cheese",
    "Japanese sushi|sushi ramen|ramen tempura|tempura kaiseki|kaiseki cuisine",
    "Mexican tacos|tacos salsa|salsa tortilla|tortilla beans|beans spicy",
    "Indian curry|curry tandoori|tandoori naan|naan spices|spices vegetarian",
    "Thai food|food pad|pad thai|thai tom|tom yum|yum coconut|coconut curry",
    "Mediterranean diet|diet olive|olive oil|oil Greek|Greek healthy",
    "American hamburger|hamburger BBQ|BBQ fast|fast food|food McDonald's",
    "Korean kimchi|kimchi BBQ|BBQ bulgogi|bulgogi fermented|fermented spicy",
    "Spanish tapas|tapas paella|paella jamon|jamon sangria|sangria cuisine",
    "Middle Eastern|Eastern hummus|hummus falafel|falafel shawarma|shawarma kebab",
    "Vietnamese pho|pho banh|banh mi|mi spring|spring rolls|rolls fish|fish sauce",
    "Ethiopian injera|injera berbere|berbere coffee|coffee ceremony",
    "Brazilian churrasco|churrasco feijoada|feijoada acai|acai caipirinha",
    "vegetarian vegan|vegan plant-based|plant-based meat|meat alternatives",
    // Human Body and Medicine (16 queries)
    "COVID-19 pandemic|pandemic vaccine|vaccine mRNA|mRNA Pfizer|Pfizer Moderna",
    "cancer types|types treatment|treatment chemotherapy|chemotherapy immunotherapy",
    "heart disease|disease cardiovascular|cardiovascular symptoms|symptoms prevention",
    "diabetes type|type 1|1 type|type 2|2 insulin|insulin blood|blood sugar",
    "brain anatomy|anatomy neurons|neurons neurotransmitters|neurotransmitters consciousness",
    "DNA genetics|genetics CRISPR|CRISPR gene|gene editing|editing heredity",
    "immune system|system antibodies|antibodies white|white blood|blood cells",
    "mental health|health depression|depression anxiety|anxiety therapy|therapy medication",
    "pregnancy childbirth|childbirth prenatal|prenatal care|care development",
    "digestive system|system stomach|stomach intestines|intestines microbiome",
    "respiratory system|system lungs|lungs breathing|breathing asthma",
    "skeletal system|system bones|bones joints|joints arthritis|arthritis osteoporosis",
    "blood types|types donation|donation transfusion|transfusion compatibility",
    "vaccines immunization|immunization schedule|schedule childhood|childhood diseases",
    "antibiotics resistance|resistance bacteria|bacteria penicillin",
    "surgery anesthesia|anesthesia minimally|minimally invasive|invasive procedures",
    // Internet and Social Media (16 queries)
    "YouTube videos|videos creators|creators monetization|monetization algorithm",
    "Instagram photos|photos stories|stories influencers|influencers filters",
    "TikTok viral|viral dances|dances trends|trends algorithm|algorithm Gen|Gen Z",
    "Reddit subreddits|subreddits upvotes|upvotes karma|karma community",
    "Wikipedia editors|editors articles|articles citations|citations neutral",
    "Twitter tweets|tweets hashtags|hashtags trending|trending retweets",
    "Facebook groups|groups marketplace|marketplace messenger|messenger Meta",
    "LinkedIn professional|professional networking|networking jobs|jobs recruitment",
    "Snapchat disappearing|disappearing messages|messages stories|stories filters",
    "Discord gaming|gaming chat|chat servers|servers voice|voice community",
    "Twitch streaming|streaming gaming|gaming just|just chatting|chatting donations",
    "Pinterest boards|boards DIY|DIY recipes|recipes visual|visual discovery",
    "WhatsApp messaging|messaging encryption|encryption groups|groups status",
    "Telegram channels|channels encryption|encryption privacy|privacy groups",
    "OnlyFans content|content creators|creators subscription|subscription adult",
    "memes viral|viral internet|internet culture|culture reaction|reaction GIFs",
    // Major Technology Companies (16 queries)
    "Apple iPhone|iPhone Steve|Steve Jobs|Jobs Tim|Tim Cook",
    "Google search|search algorithm|algorithm PageRank|PageRank advertising",
    "Microsoft Windows|Windows Office|Office Azure|Azure cloud",
    "Amazon e-commerce|e-commerce AWS|AWS Jeff|Jeff Bezos",
    "Meta Facebook|Facebook social|social media|media Mark|Mark Zuckerberg",
    "Tesla electric|electric vehicles|vehicles Elon|Elon Musk|Musk autopilot",
    "Netflix streaming|streaming content|content original|original series",
    "Samsung electronics|electronics smartphones|smartphones semiconductors",
    "Intel processors|processors CPU|CPU manufacturing|manufacturing chips",
    "NVIDIA graphics|graphics cards|cards GPU|GPU AI|AI computing",
    "Twitter X|X platform|platform tweets|tweets social|social media",
    "TikTok short|short video|video algorithm|algorithm ByteDance",
    "Spotify music|music streaming|streaming podcasts|podcasts playlists",
    "Adobe Creative|Creative Cloud|Cloud Photoshop|Photoshop software",
    "Oracle database|database enterprise|enterprise software|software cloud",
    "IBM Watson|Watson AI|AI mainframe|mainframe computers|computers history",
    // Major World Cities (16 queries)
    "Tokyo metropolitan|metropolitan area|area population|population density",
    "New York|York City|City boroughs|boroughs Manhattan|Manhattan Brooklyn",
    "London Underground|Underground tube|tube map|map zones",
    "Paris arrondissements|arrondissements Eiffel|Eiffel Tower|Tower Seine",
    "Singapore urban|urban planning|planning public|public housing",
    "Dubai skyscrapers|skyscrapers Burj|Burj Khalifa|Khalifa construction",
    "Sao Paulo|Paulo Brazil|Brazil favelas|favelas urban|urban sprawl",
    "Mumbai Bollywood|Bollywood film|film industry|industry slums",
    "Beijing Forbidden|Forbidden City|City air|air pollution",
    "Los Angeles|Angeles Hollywood|Hollywood entertainment|entertainment industry",
    "Moscow Kremlin|Kremlin Red|Red Square|Square metro",
    "Cairo Egypt|Egypt pyramids|pyramids Nile|Nile River",
    "Istanbul Bosphorus|Bosphorus Europe|Europe Asia|Asia bridge",
    "Sydney Opera|Opera House|House Harbor|Harbor Bridge",
    "Berlin Wall|Wall Cold|Cold War|War reunification",
    "Mexico City|City Aztec|Aztec Tenochtitlan|Tenochtitlan altitude",
    // Popular Films and Cinema (16 queries)
    "Marvel Cinematic|Cinematic Universe|Universe timeline",
    "Star Wars|Wars franchise|franchise box|box office",
    "Academy Awards|Awards Best|Best Picture|Picture winners",
    "Christopher Nolan|Nolan filmography|filmography cinematography",
    "Studio Ghibli|Ghibli animated|animated films|films Miyazaki",
    "Quentin Tarantino|Tarantino dialogue|dialogue style|style violence",
    "James Bond|Bond 007|007 actors|actors chronology",
    "Pixar Animation|Animation Studios|Studios technology|technology CGI",
    "Lord of|of the|the Rings|Rings trilogy|trilogy extended|extended editions",
    "Harry Potter|Potter film|film series|series cast",
    "The Godfather|Godfather trilogy|trilogy mafia|mafia crime",
    "Matrix franchise|franchise philosophy|philosophy cyberpunk",
    "Disney animated|animated films|films Renaissance|Renaissance era",
    "horror movie|movie franchises|franchises Halloween|Halloween Friday",
    "superhero movie|movie DC|DC Comics|Comics Batman",
    "streaming service|service Netflix|Netflix original|original films",
    // Popular Music Artists (16 queries)
    "The Beatles|Beatles albums|albums Abbey|Abbey Road|Road Liverpool",
    "Taylor Swift|Swift Eras|Eras Tour|Tour album|album releases",
    "Michael Jackson|Jackson King|King of|of Pop|Pop Thriller|Thriller moonwalk",
    "Beyonce Formation|Formation Destiny's|Destiny's Child|Child solo|solo career",
    "Drake Toronto|Toronto rap|rap streaming|streaming records",
    "Eminem rap|rap battle|battle 8|8 Mile|Mile Detroit",
    "Madonna pop|pop icon|icon reinvention|reinvention tours",
    "Elvis Presley|Presley Graceland|Graceland rock|rock and|and roll",
    "Bob Dylan|Dylan Nobel|Nobel Prize|Prize literature|literature folk",
    "Kanye West|West Yeezy|Yeezy fashion|fashion producer|producer rapper",
    "Lady Gaga|Gaga pop|pop art|art performance|performance costumes",
    "Ed Sheeran|Sheeran songwriting|songwriting acoustic|acoustic guitar|guitar tours",
    "Rihanna Fenty|Fenty beauty|beauty business|business music",
    "Queen Freddie|Freddie Mercury|Mercury Bohemian|Bohemian Rhapsody",
    "Pink Floyd|Floyd The|The Wall|Wall Dark|Dark Side|Side Moon",
    "BTS K-pop|K-pop global|global phenomenon|phenomenon Army|Army fandom",
    // Professional Sports (16 queries)
    "FIFA World|World Cup|Cup tournament|tournament history|history winners",
    "NBA basketball|basketball playoffs|playoffs championship|championship rings",
    "NFL Super|Super Bowl|Bowl halftime|halftime show|show commercials",
    "Premier League|League football|football Manchester|Manchester United|United Liverpool",
    "Olympics Summer|Summer Winter|Winter medal|medal count",
    "Tennis Grand|Grand Slam|Slam Wimbledon|Wimbledon Federer|Federer Nadal",
    "Formula 1|1 racing|racing drivers|drivers championship",
    "MLB World|World Series|Series Yankees|Yankees baseball|baseball statistics",
    "UEFA Champions|Champions League|League Real|Real Madrid|Madrid Barcelona",
    "cricket Test|Test matches|matches India|India Australia|Australia Ashes",
    "golf Masters|Masters Tournament|Tournament Augusta|Augusta Tiger|Tiger Woods",
    "boxing heavyweight|heavyweight championship|championship Muhammad|Muhammad Ali",
    "NHL hockey|hockey Stanley|Stanley Cup|Cup playoffs",
    "Tour de|de France|France cycling|cycling yellow|yellow jersey",
    "rugby World|World Cup|Cup All|All Blacks|Blacks New|New Zealand",
    "MMA UFC|UFC fighters|fighters championship|championship belts",
    // Programming Languages (16 queries)
    "Python machine|machine learning|learning data|data science|science syntax",
    "JavaScript web|web development|development Node.js|Node.js React",
    "Java enterprise|enterprise Android|Android object-oriented|object-oriented JVM",
    "C++ performance|performance systems|systems programming|programming STL",
    "C programming|programming language|language Unix|Unix Linux|Linux kernel",
    "TypeScript JavaScript|JavaScript types|types Angular|Angular development",
    "Rust memory|memory safety|safety systems|systems programming|programming Mozilla",
    "Go Google|Google concurrency|concurrency cloud|cloud native|native Docker",
    "Swift iOS|iOS Apple|Apple development|development Xcode|Xcode mobile",
    "Kotlin Android|Android JetBrains|JetBrains Java|Java interoperability",
    "PHP web|web development|development WordPress|WordPress Laravel|Laravel server",
    "Ruby Rails|Rails web|web framework|framework convention|convention configuration",
    "SQL database|database queries|queries relational|relational PostgreSQL|PostgreSQL MySQL",
    "R statistics|statistics data|data analysis|analysis visualization|visualization packages",
    "MATLAB numerical|numerical computing|computing engineering|engineering simulation",
    "Bash shell|shell scripting|scripting Linux|Linux automation|automation terminal",
    // Space and Astronomy (16 queries)
    "International Space|Space Station|Station ISS|ISS astronauts|astronauts orbit",
    "Mars rover|rover Perseverance|Perseverance Curiosity|Curiosity NASA|NASA exploration",
    "black holes|holes event|event horizon|horizon Stephen|Stephen Hawking",
    "James Webb|Webb Space|Space Telescope|Telescope infrared|infrared universe",
    "SpaceX Falcon|Falcon rockets|rockets Starship|Starship Mars|Mars Elon|Elon Musk",
    "Moon landing|landing Apollo|Apollo 11|11 Neil|Neil Armstrong|Armstrong 1969",
    "Hubble Space|Space Telescope|Telescope images|images discoveries|discoveries galaxies",
    "solar system|system planets|planets Jupiter|Jupiter Saturn|Saturn rings",
    "Milky Way|Way galaxy|galaxy spiral|spiral arms|arms black|black hole|hole center",
    "exoplanets habitable|habitable zone|zone Kepler|Kepler TESS|TESS discovery",
    "Big Bang|Bang theory|theory universe|universe origin|origin cosmic|cosmic inflation",
    "dark matter|matter dark|dark energy|energy universe|universe expansion",
    "asteroids comets|comets meteors|meteors impact|impact Earth|Earth defense",
    "constellation stars|stars navigation|navigation mythology|mythology patterns",
    "NASA missions|missions Artemis|Artemis program|program return|return Moon",
    "supernova star|star explosion|explosion neutron|neutron star|star pulsar",
    // Television Series (16 queries)
    "Game of|of Thrones|Thrones HBO|HBO dragons|dragons Iron|Iron Throne",
    "Breaking Bad|Bad Walter|Walter White|White meth|meth Albuquerque",
    "The Office|Office mockumentary|mockumentary Dunder|Dunder Mifflin|Mifflin comedy",
    "Friends Central|Central Perk|Perk Ross|Ross Rachel|Rachel sitcom",
    "Stranger Things|Things Netflix|Netflix 80s|80s Upside|Upside Down",
    "The Simpsons|Simpsons Springfield|Springfield Homer|Homer animation|animation longest",
    "The Crown|Crown Netflix|Netflix Queen|Queen Elizabeth|Elizabeth royal|royal family",
    "The Sopranos|Sopranos HBO|HBO mafia|mafia Tony|Tony therapy",
    "House of|of Cards|Cards political|political drama|drama Netflix|Netflix Spacey",
    "The Walking|Walking Dead|Dead zombies|zombies Rick|Rick survival|survival AMC",
    "Black Mirror|Mirror dystopia|dystopia technology|technology anthology|anthology Netflix",
    "The Big|Big Bang|Bang Theory|Theory nerds|nerds Sheldon|Sheldon physics|physics sitcom",
    "Succession HBO|HBO family|family business|business media|media empire",
    "The Mandalorian|Mandalorian Star|Star Wars|Wars Baby|Baby Yoda|Yoda Disney+",
    "Squid Game|Game Korean|Korean Netflix|Netflix survival|survival games",
    "Westworld HBO|HBO AI|AI robots|robots western|western sci-fi",
    // Video Games and Gaming (16 queries)
    "Nintendo Mario|Mario Zelda|Zelda franchise|franchise history",
    "PlayStation exclusive|exclusive games|games Sony|Sony consoles",
    "Xbox Game|Game Pass|Pass Microsoft|Microsoft acquisition|acquisition Activision",
    "Steam PC|PC gaming|gaming Valve|Valve Half-Life|Half-Life Portal",
    "Minecraft crafting|crafting survival|survival creative|creative mode",
    "Grand Theft|Theft Auto|Auto open|open world|world Rockstar",
    "Call of|of Duty|Duty multiplayer|multiplayer battle|battle royale",
    "Pokemon generations|generations trading|trading card|card game",
    "League of|of Legends|Legends esports|esports championships|championships MOBA",
    "Fortnite battle|battle royale|royale building|building mechanics",
    "World of|of Warcraft|Warcraft MMORPG|MMORPG expansion|expansion packs",
    "The Elder|Elder Scrolls|Scrolls Skyrim|Skyrim modding|modding community",
    "FIFA football|football simulation|simulation Ultimate|Ultimate Team",
    "Assassin's Creed|Creed historical|historical settings|settings Ubisoft",
    "Dark Souls|Souls difficulty|difficulty FromSoftware|FromSoftware gameplay",
    "mobile gaming|gaming Candy|Candy Crush|Crush Angry|Angry Birds",
    // World War II History (16 queries)
    "D-Day Normandy|Normandy invasion|invasion Allied|Allied forces|forces beaches",
    "Holocaust concentration|concentration camps|camps genocide|genocide Jews",
    "Pearl Harbor|Harbor December|December 7|7 1941|1941 Japan|Japan attack",
    "Adolf Hitler|Hitler Nazi|Nazi Germany|Germany Third|Third Reich",
    "Winston Churchill|Churchill Britain|Britain Battle|Battle of|of Britain",
    "atomic bomb|bomb Hiroshima|Hiroshima Nagasaki|Nagasaki Manhattan|Manhattan Project",
    "Battle of|of Stalingrad|Stalingrad Eastern|Eastern Front|Front Soviet|Soviet Union",
    "Franklin D|D Roosevelt|Roosevelt New|New Deal|Deal president",
    "Blitzkrieg Poland|Poland invasion|invasion September|September 1939",
    "Anne Frank|Frank diary|diary Amsterdam|Amsterdam hiding|hiding Holocaust",
    "Battle of|of Midway|Midway Pacific|Pacific naval|naval turning|turning point",
    "Auschwitz concentration|concentration camp|camp liberation|liberation prisoners",
    "Joseph Stalin|Stalin Soviet|Soviet Union|Union purges|purges leadership",
    "resistance movements|movements French|French underground|underground partisans",
    "Nuremberg trials|trials war|war crimes|crimes justice|justice defendants",
    "Japanese internment|internment camps|camps United|United States",
];

const NUM_DOCUMENTS: usize = 128 * 1024; // Default number of documents to index
const MAX_DOCUMENT_LENGTH: usize = 8192; // Maximum document length (8K chars)
const BATCH_SIZE: usize = 65536; // Number of documents to index per batch

fn bench_fts_regex_candidates(c: &mut Criterion) {
    let runner = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create runtime");

    // Load documents using streaming - only what we need
    let documents = runner.block_on(async {
        use futures::StreamExt;

        let dataset = WikipediaSplade::init().await.unwrap();
        let doc_stream = dataset.documents().await.unwrap();

        let mut docs = Vec::with_capacity(NUM_DOCUMENTS);
        let mut stream = Box::pin(doc_stream);

        while let Some(doc_result) = stream.next().await {
            if docs.len() >= NUM_DOCUMENTS {
                break;
            }

            if let Ok(doc) = doc_result {
                let combined = format!("{} {}", doc.title, doc.body);
                if combined.len() <= MAX_DOCUMENT_LENGTH {
                    docs.push(combined);
                }
            }
        }

        docs
    });

    let (_temp_dir, blockfile_provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

    // Build FTS index incrementally
    let index_reader = runner.block_on(async {
        let prefix_path = String::from("");
        let mut writer_id = None;
        let mut current_offset = 0u32;

        // Process documents in batches
        for chunk in documents.chunks(BATCH_SIZE) {
            // Create writer options, forking if not the first chunk
            let mut writer_options = BlockfileWriterOptions::new(prefix_path.clone());
            if let Some(id) = writer_id {
                writer_options = writer_options.fork(id);
            }

            let writer = blockfile_provider
                .write::<u32, Vec<u32>>(writer_options)
                .await
                .unwrap();

            let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
            let mut fts_writer = FullTextIndexWriter::new(writer, tokenizer);

            // Create mutations for this batch
            let mutations: Vec<_> = chunk
                .iter()
                .enumerate()
                .map(|(i, doc)| DocumentMutation::Create {
                    offset_id: current_offset + i as u32,
                    new_document: doc.as_str(),
                })
                .collect();

            current_offset += chunk.len() as u32;

            fts_writer.handle_batch(mutations).unwrap();
            fts_writer.write_to_blockfiles().await.unwrap();
            let flusher = fts_writer.commit().await.unwrap();
            writer_id = Some(flusher.pls_id());
            flusher.flush().await.unwrap();
        }

        // Create reader from final writer ID
        let reader = blockfile_provider
            .read::<u32, &[u32]>(BlockfileReaderOptions::new(
                writer_id.expect("No documents indexed"),
                prefix_path,
            ))
            .await
            .unwrap();

        let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
        FullTextIndexReader::new(reader, tokenizer)
    });

    // Benchmark regex candidate selection (Phase 1 only)
    let mut group = c.benchmark_group("fts_regex_candidates");

    // Configure for longer runs (optional - can be overridden by CLI args)
    // group.warm_up_time(std::time::Duration::from_secs(10));
    // group.measurement_time(std::time::Duration::from_secs(30));
    // group.sample_size(500);

    // Test all queries with alternation patterns
    for (i, query) in QUERIES.iter().enumerate() {
        // Use first 30 chars of query as benchmark name
        let bench_name = if query.len() > 30 {
            format!("{:03}_{}", i, &query[..30])
        } else {
            format!("{:03}_{}", i, query)
        };

        group.bench_function(BenchmarkId::from_parameter(bench_name), |b| {
            b.to_async(&runner).iter(|| async {
                let regex = ChromaRegex::try_from(query.to_string()).unwrap();
                let literal_expr = LiteralExpr::from(regex.hir().clone());
                let _candidates = index_reader
                    .match_literal_expression(&literal_expr)
                    .await
                    .unwrap()
                    .unwrap_or_default();
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_fts_regex_candidates);
criterion_main!(benches);
