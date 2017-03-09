create table customers (
	sell_to_customer_no_ text(15) primary key,
	last_order_date text(60),
	phone_no_ text(60),
	current_med_advantage text(10),
	sell_to_customer_name text(60),
	ship_to_post_code text(60),
	usa_email text(60),
	total_number_of_orders text(5),
	segment text(15),
	record_create_date text(15),
	file_parse_date text(15)
);
