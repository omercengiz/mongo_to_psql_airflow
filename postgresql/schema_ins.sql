CREATE TABLE shipments (
    shipment_id VARCHAR(255) NOT NULL,
    shipment_date TIMESTAMP WITHOUT TIME ZONE,
    parcels JSONB,
    address JSONB,
    PRIMARY KEY (shipment_id)
);