CREATE TABLE IF NOT EXISTS `ap_revenue` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `ticket` INT,
    `pay_station` VARCHAR(15),
    `stall` INT,
    `license_plate` VARCHAR (8),
    `day` ENUM('Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'),
    `date` DATE,
    `time` TIME,
    `purchased_date` DATETIME,
    `expiry_date` DATETIME,
    `payment_type` VARCHAR(15),
    `transaction_type` VARCHAR(15),
    `coupon_code` VARCHAR(15),
    `excess_payment` DECIMAL(10,2),
    `change_issued` DECIMAL(10,2),
    `refund_ticket` DECIMAL(10,2),
    `total_collections` DECIMAL(10,2),
    `revenue` DECIMAL(10,2),
    `rate_name` TINYINT,
    `hours_paid` DECIMAL(4,2) UNSIGNED,
    `zone` VARCHAR(15),
    `new_rate_weekday` DOUBLE,
    `new_revenue_weekday` DOUBLE,
    `new_rate_weekend` DOUBLE,
    `new_revenue_weekend` DOUBLE,
     PRIMARY KEY (`id`)
);