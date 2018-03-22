CREATE TABLE IF NOT EXISTS `ap_transactions` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `ticket` INT,
    `pay_station` VARCHAR(50),
    `stall` VARCHAR(50),
    `license_plate` VARCHAR(8),
    `day` ENUM('Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'),
    `date` DATE,
    `time` TIME,
    `purchased_date` DATETIME NOT NULL,
    `expiry_date` DATETIME,
    `payment_type` VARCHAR(50),
    `transaction_type` VARCHAR(50),
    `coupon_code` VARCHAR(50),
    `excess_payment` DECIMAL(10,2),
    `change_issued` DECIMAL(10,2),
    `refund_ticket` DECIMAL(10,2),
    `total_collections` DECIMAL(10,2),
    `revenue` DECIMAL(10,2),
    `rate_name` VARCHAR(50),
    `hours_paid` DECIMAL(4,2) UNSIGNED,
    `zone` VARCHAR(50),
    `new_rate_weekday` DOUBLE,
    `new_revenue_weekday` DOUBLE,
    `new_rate_weekend` DOUBLE,
    `new_revenue_weekend` DOUBLE,
    `passport_tran` INT,
    `merchant_tran` BIGINT,
    `parker_id` INT,
    `conv_revenue` DECIMAL (10,2),
    `validation_revenue` DECIMAL (10,2),
    `transaction_fee` DECIMAL (10,2),
    `card_type` VARCHAR(50),
    `method` VARCHAR(50),
     PRIMARY KEY (`id`)
);
