package com.shell.custom.datatype.example1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Vehicle implements Writable {
	private String model;
	private String vin;
	private int mileage;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(mileage);
		out.writeUTF(model);
		out.writeUTF(vin);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mileage = in.readInt();
		model = in.readUTF();
		vin = in.readUTF();
	}

	public String getModel() {
		return model;
	}

	public void setModel(String model) {
		this.model = model;
	}

	public String getVin() {
		return vin;
	}

	public void setVin(String vin) {
		this.vin = vin;
	}

	public int getMileage() {
		return mileage;
	}

	public void setMileage(int mileage) {
		this.mileage = mileage;
	}

	@Override
	public String toString() {
		return model + ", " + vin + ", " + mileage;
	}
}
