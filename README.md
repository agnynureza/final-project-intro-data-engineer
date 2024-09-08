# Proyek Akhir: Introduction to Data Engineering

## Requirements Gathering & Solution

### Data Source
1. Data penjualan: Diperoleh dari Docker image https://hub.docker.com/r/shandytp/amazon-sales-data-docker-db
   - Untuk menjalankan kontainer Docker: `docker run --platform linux/amd64 -d -p 5433:5432 shandytp/amazon-sales-data-docker-db:latest`
2. Data pemasaran: Bersumber dari file CSV `ElectronicsProductsPricingData.csv`
3. Data universitas: Diambil dari REST API http://universities.hipolabs.com/search?country=Indonesia, yang menyediakan data untuk universitas-universitas di seluruh Indonesia

### Problem
1. Tim Penjualan & Pemasaran menghadapi kesulitan dalam memperoleh data produk karena data tersebar di berbagai file, sehingga sulit untuk menganalisis kinerja produk.
2. Tim Penjualan ingin mengidentifikasi produk dengan rating pelanggan tertinggi dari data penjualan.
3. Tim Pemasaran ingin menentukan produk elektronik yang paling populer di setiap kategori.
4. Tim Penjualan tertarik untuk memahami pola domain yang umum digunakan oleh universitas-universitas di Indonesia.

### Solution
1. Melakukan proses ETL (Extract, Transform, Load) untuk menggabungkan semua data ke dalam satu database.
2. Menggunakan Luigi sebagai penjadwal alur kerja
3. Menentukan skema gudang data yang sesuai dengan kebutuhan bisnis dan memungkinkan analisis setiap masalah.
4. Mengimplementasikan penjadwal untuk memastikan pembaruan data secara berkelanjutan dari berbagai sumber ke gudang data.
5. Mengimplementasikan proses upsert untuk menangani perubahan pada tabel-tabel.

### Schema Data Warehouse

1. Untuk tabel `amazon_sales_data`:
   - id (primary key)
   - product_name
   - main_category
   - sub_category
   - ratings
   - number_of_ratings
   - discount_price
   - actual_size
2. Untuk tabel `electronic_product_pricing_data`:
   - id (primary key)
   - category_name
   - product_name
   - upc
   - weight
   - price
   - currency
   - availability
   - condition
   - manufacturer
   - is_sale
3. Untuk tabel `universities`:
   - id (primary key)
   - name
   - country
   - alpha_two_code
   - domain
   - web_page_url

## ETL Processing

### Langkah-langkah yang terlibat dalam proses ETL
1. Mengekstrak data dari berbagai sumber dan memuat data tersebut ke area staging dalam format CSV.
2. Melakukan validasi data dengan memeriksa bentuk data, tipe data, dan menangani nilai yang hilang.
3. Menerapkan transformasi jika diperlukan, seperti menggabungkan tabel dan membersihkan data dengan memeriksa nama kolom, nilai di setiap kolom, dan memilih kolom yang relevan untuk disimpan di gudang data.
4. Memuat semua data yang telah ditransformasi ke gudang data dalam satu database.

### Desain Alur ETL

Diagram berikut menggambarkan desain alur ETL:

![diagram](img/diagram.png)

Desain alur ETL terdiri dari komponen-komponen berikut:

1. **Extract**: Tahap ini mengambil data dari berbagai sumber, termasuk file CSV produk elektronik, API universitas, dan Docker DB penjualan Amazon.

2. **Validation**: Data yang diekstrak menjalani pemeriksaan validasi, termasuk memverifikasi bentuk data, tipe data, dan menangani nilai yang hilang.

3. **Transform**: Pada tahap ini, tugas-tugas transformasi data dilakukan, seperti menerapkan teknik pembersihan data.

4. **Load**: Data yang telah ditransformasi dimuat ke data warehouse untuk analisis lebih lanjut.

5. **Extract Directory**: Ini merepresentasikan direktori tempat data yang diekstrak disimpan sebelum diolah.

Tanda panah menunjukkan aliran data antara berbagai tahap alur ETL.

Desain keseluruhan di jalankan dengan luigi dan proses insert data kedalam data warehouse dilakukan dengan metode upsert, kemudian di set proses scheduling dengan menggunakan crontab

### Testing Scenario 1
1. dalam scenario pertama ini raw data di load ke data warehouse dengan menggunakan luigi untuk workflow dependency nya
![cmd](img/testing_scenario_1_cmd.png)

2. contoh salah satu data csv amazon sales
![csv](img/testing_scenario_1_csv.png)

3. contoh data yang masuk ke data data warehouse di table amazon sales
![db](img/testing_scenario_1_db.png)

### Testing Scenario 2
1. dalam scenario kedua terlihat hanya beberapa proses saja yang jalan karena hanya dirubah data csv yang amazon sales
![cmd](img/testing_scenario_2_cmd.png)

2. contoh salah satu data csv amazon sales yang dirubah
![csv](img/testing_scenario_2_csv.png)

3. contoh data yang masuk ke data data warehouse di table amazon sales ketika dirubah
![db](img/testing_scenario_2_db.png)


### Set up Crontab
1. type `crontab -e`
2. set the timer and path to the runner file
![cron](img/crontab.png)

### Running from Dockerfile
1. type `docker build -t final-data-engineering .`
2. type `docker run engineering`

## Completed

- [x] Requirements Gathering & Solution
- [x] ETL Pipeline Design:
  - [x] Membuat Design ETL Pipeline
  - [x] Penjelasan mengenai Design ETL Pipeline
- [x] ETL Implementation:
  - [x] Membuat ETL Pipeline
  - [x] Extract seluruh source data
  - [x] ETL Scheduling
  - [x] Membuat Dockerfile / Docker Image
- [x] Testing Scenario
- [x] Publikasi Project di Github
- [ ] Video Presentasi