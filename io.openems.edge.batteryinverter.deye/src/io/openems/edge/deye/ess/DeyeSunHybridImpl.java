package io.openems.edge.deye.ess;

import io.openems.common.channel.AccessMode;
import io.openems.common.exceptions.OpenemsError.OpenemsNamedException;
import io.openems.common.exceptions.OpenemsException;
import io.openems.edge.bridge.modbus.api.AbstractOpenemsModbusComponent;
import io.openems.edge.bridge.modbus.api.BridgeModbus;
import io.openems.edge.bridge.modbus.api.ModbusComponent;
import io.openems.edge.bridge.modbus.api.ModbusProtocol;
import io.openems.edge.bridge.modbus.api.element.DummyRegisterElement;
import io.openems.edge.bridge.modbus.api.element.SignedWordElement;
import io.openems.edge.bridge.modbus.api.element.StringWordElement;
import io.openems.edge.bridge.modbus.api.element.UnsignedWordElement;
import io.openems.edge.bridge.modbus.api.task.FC16WriteRegistersTask;
import io.openems.edge.bridge.modbus.api.task.FC3ReadRegistersTask;
import io.openems.edge.common.channel.EnumWriteChannel;
import io.openems.edge.common.channel.IntegerWriteChannel;
import io.openems.edge.common.channel.StateChannel;
import io.openems.edge.common.channel.IntegerReadChannel;
import io.openems.edge.common.component.ComponentManager;
import io.openems.edge.common.component.OpenemsComponent;
import io.openems.edge.common.event.EdgeEventConstants;
import io.openems.edge.common.modbusslave.ModbusSlave;
import io.openems.edge.common.modbusslave.ModbusSlaveTable;
import io.openems.edge.common.taskmanager.Priority;
import io.openems.edge.common.sum.GridMode;
import io.openems.edge.ess.api.HybridEss;
import io.openems.edge.ess.api.ManagedSymmetricEss;
import io.openems.edge.ess.api.SymmetricEss;
import io.openems.edge.ess.power.api.Constraint;
import io.openems.edge.ess.power.api.Phase;
import io.openems.edge.ess.power.api.Power;
import io.openems.edge.ess.power.api.Pwr;
import io.openems.edge.ess.power.api.Relationship;
import io.openems.edge.timedata.api.Timedata;
import io.openems.edge.timedata.api.TimedataProvider;
import io.openems.edge.timedata.api.utils.CalculateEnergyFromPower;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.osgi.service.event.propertytypes.EventTopics;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

import io.openems.edge.controller.ess.timeofusetariff;

@Designate(ocd = Config.class, factory = true)
@Component(//
		name = "Deye.BatteryInverter", //
		immediate = true, //
		configurationPolicy = ConfigurationPolicy.REQUIRE //
)
@EventTopics({ //
		EdgeEventConstants.TOPIC_CYCLE_AFTER_PROCESS_IMAGE, //
		EdgeEventConstants.TOPIC_CYCLE_BEFORE_CONTROLLERS //
})
public class DeyeSunHybridImpl extends AbstractOpenemsModbusComponent implements DeyeSunHybrid, ManagedSymmetricEss,
		SymmetricEss, ModbusComponent, OpenemsComponent, EventHandler, ModbusSlave, TimedataProvider {

	protected static final int MAX_APPARENT_POWER = 40000;

	protected static final int NET_CAPACITY = 28000;

	private static final int MIN_REACTIVE_POWER = -10000;

	private static final int MAX_REACTIVE_POWER = 10000;

	private final Logger log = LoggerFactory.getLogger(DeyeSunHybridImpl.class);

	private final CalculateEnergyFromPower calculateAcChargeEnergy = new CalculateEnergyFromPower(this,
			SymmetricEss.ChannelId.ACTIVE_CHARGE_ENERGY);

	private final CalculateEnergyFromPower calculateAcDischargeEnergy = new CalculateEnergyFromPower(this,
			SymmetricEss.ChannelId.ACTIVE_DISCHARGE_ENERGY);

	private final CalculateEnergyFromPower calculateDcChargeEnergy = new CalculateEnergyFromPower(this,
			HybridEss.ChannelId.DC_CHARGE_ENERGY);

	private final CalculateEnergyFromPower calculateDcDischargeEnergy = new CalculateEnergyFromPower(this,
			HybridEss.ChannelId.DC_DISCHARGE_ENERGY);

	
	@Reference
	private ComponentManager componentManager;

	@Reference
	private Power power;

	@Reference
	private ConfigurationAdmin cm;

	@Override
	@Reference(policy = ReferencePolicy.STATIC, policyOption = ReferencePolicyOption.GREEDY, cardinality = ReferenceCardinality.MANDATORY)
	protected void setModbus(BridgeModbus modbus) {
		super.setModbus(modbus);
	}

	@Reference(policy = ReferencePolicy.DYNAMIC, policyOption = ReferencePolicyOption.GREEDY, cardinality = ReferenceCardinality.OPTIONAL)
	private volatile Timedata timedata = null;

	@Reference(policy = ReferencePolicy.DYNAMIC, policyOption = ReferencePolicyOption.GREEDY, cardinality = ReferenceCardinality.OPTIONAL)
	private TimeOfUseController timeOfUseController;

	private Config config;

	public DeyeSunHybridImpl() {
		super(//
				OpenemsComponent.ChannelId.values(), //
				ModbusComponent.ChannelId.values(), //
				SymmetricEss.ChannelId.values(), //
				ManagedSymmetricEss.ChannelId.values(), //
				HybridEss.ChannelId.values(), //
				DeyeSunHybrid.SystemErrorChannelId.values(), //
				DeyeSunHybrid.InsufficientGridParametersChannelId.values(), //
				DeyeSunHybrid.PowerDecreaseCausedByOvertemperatureChannelId.values(), //
				DeyeSunHybrid.ChannelId.values() //
		);
		this._setCapacity(NET_CAPACITY);
	}

	@Activate
	private void activate(ComponentContext context, Config config) throws OpenemsException {
		if (super.activate(context, config.id(), config.alias(), config.enabled(), config.unit_id(), this.cm, "Modbus",
				config.modbus_id())) {
			return;
		}
		//System.out.println("*** CHAOSMAN 1 ***");
		this.config = config;
		this._setAllowedChargePower(-12000);
		this._setAllowedDischargePower(12000);
		this._setMaxApparentPower(2000);
		this._setGridMode(GridMode.ON_GRID);
	}

	@Override
	@Deactivate
	protected void deactivate() {
		super.deactivate();
	}

	@Override
	public void applyPower(int activePower, int reactivePower) throws OpenemsNamedException {
		System.out.println("CHAOSMAN activePower: " + activePower + " reactivePower: " + reactivePower);
		if (this.config.readOnlyMode()) {
			return;
		}

		//IntegerWriteChannel setActivePowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_ACTIVE_POWER);
		//setActivePowerChannel.setNextWriteValue(activePower);
		//IntegerReadChannel channel = this.channel(DeyeSunHybrid.ChannelId.SET_GRID_CHARGING_START_CAPACITY_POINT);
		//System.out.println("*** channel.value(): " + channel.value().orElse(0));
		//if (channel.value().orElse(0) < 100) {
		//	IntegerWriteChannel setGridChargingCapacityPointChannel = this.channel(DeyeSunHybrid.ChannelId.SET_GRID_CHARGING_START_CAPACITY_POINT);
		//	setGridChargingCapacityPointChannel.setNextWriteValue(100);
		//}
		/*if (activePower < 0) {
			IntegerWriteChannel setSellMode1PowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_SELL_MODE_1_POWER);
			IntegerWriteChannel setSellMode2PowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_SELL_MODE_2_POWER);
			IntegerWriteChannel setSellMode3PowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_SELL_MODE_3_POWER);
			IntegerWriteChannel setSellMode4PowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_SELL_MODE_4_POWER);
			IntegerWriteChannel setSellMode5PowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_SELL_MODE_5_POWER);
			IntegerWriteChannel setSellMode6PowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_SELL_MODE_6_POWER);
			setSellMode1PowerChannel.setNextWriteValue(activePower * -1);
			setSellMode2PowerChannel.setNextWriteValue(activePower * -1);
			setSellMode3PowerChannel.setNextWriteValue(activePower * -1);
			setSellMode4PowerChannel.setNextWriteValue(activePower * -1);
			setSellMode5PowerChannel.setNextWriteValue(activePower * -1);
			setSellMode6PowerChannel.setNextWriteValue(activePower * -1);
		} */
		if (activePower < 0) {
			IntegerWriteChannel setTimeOfUseSellingChannel = this.channel(DeyeSunHybrid.ChannelId.SET_TIME_OF_USE_SELLING);
			setTimeOfUseSellingChannel.setNextWriteValue(0);
		} else if (activePower == 0) {
			IntegerWriteChannel setTimeOfUseSellingChannel = this.channel(DeyeSunHybrid.ChannelId.SET_TIME_OF_USE_SELLING);
			setTimeOfUseSellingChannel.setNextWriteValue(255);
		}
		//setActivePowerChannel.setNextWriteValue(100);
		//IntegerWriteChannel setReactivePowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_REACTIVE_POWER);
		//setReactivePowerChannel.setNextWriteValue(reactivePower);

		// AC 1/28/2024
		//IntegerWriteChannel setGridLoadOffPowerChannel = this.channel(DeyeSunHybrid.ChannelId.SET_GRID_LOAD_OFF_POWER);
		//setGridLoadOffPowerChannel.setNextWriteValue(93);
	}

	@Override
	public String getModbusBridgeId() {
		return this.config.modbus_id();
	}

	@Override
	protected ModbusProtocol defineModbusProtocol() {
		return new ModbusProtocol(this, //

				new FC3ReadRegistersTask(1, Priority.LOW,
						m(SymmetricEss.ChannelId.GRID_MODE, new UnsignedWordElement(1)), new DummyRegisterElement(2),
						m(DeyeSunHybrid.ChannelId.SERIAL_NUMBER, new StringWordElement(3, 5))),

				new FC16WriteRegistersTask(77, m(DeyeSunHybrid.ChannelId.SET_ACTIVE_POWER, new SignedWordElement(77)),
						m(DeyeSunHybrid.ChannelId.SET_REACTIVE_POWER, new SignedWordElement(78))),
				new FC16WriteRegistersTask(127, m(DeyeSunHybrid.ChannelId.SET_GRID_CHARGING_START_CAPACITY_POINT, new SignedWordElement(127))),
				//new FC3ReadRegistersTask(127, Priority.LOW, m(DeyeSunHybrid.ChannelId.SET_GRID_CHARGING_START_CAPACITY_POINT, new SignedWordElement(127))),
				new FC16WriteRegistersTask(146, m(DeyeSunHybrid.ChannelId.SET_TIME_OF_USE_SELLING, new UnsignedWordElement(146))),
				new FC16WriteRegistersTask(154, m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_1_POWER, new SignedWordElement(154)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_2_POWER, new SignedWordElement(155)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_3_POWER, new SignedWordElement(156)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_4_POWER, new SignedWordElement(157)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_5_POWER, new SignedWordElement(158)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_6_POWER, new SignedWordElement(159))),
				new FC16WriteRegistersTask(166, m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_1_CAPACITY, new SignedWordElement(166)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_2_CAPACITY, new SignedWordElement(167)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_3_CAPACITY, new SignedWordElement(168)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_4_CAPACITY, new SignedWordElement(169)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_5_CAPACITY, new SignedWordElement(170)),
						m(DeyeSunHybrid.ChannelId.SET_SELL_MODE_6_CAPACITY, new SignedWordElement(171))),

				new FC3ReadRegistersTask(588, Priority.HIGH,
						m(SymmetricEss.ChannelId.SOC, new UnsignedWordElement(588))),

				new FC3ReadRegistersTask(500, Priority.LOW,
						m(DeyeSunHybrid.ChannelId.INVERTER_RUN_STATE, new UnsignedWordElement(500))),

				// 590
				new FC3ReadRegistersTask(590, Priority.HIGH,
						m(SymmetricEss.ChannelId.ACTIVE_POWER, new SignedWordElement(590))),

				new FC3ReadRegistersTask(620, Priority.LOW, //
						m(DeyeSunHybrid.ChannelId.APPARENT_POWER, new UnsignedWordElement(620)))

		);
	}

	@Override
	public String debugLog() {
		return "SoC:" + this.getSoc().asString() //
				+ "|L:" + this.getActivePower().asString() //
				+ "|Active Power:" + this.channel(SymmetricEss.ChannelId.ACTIVE_POWER).value().asStringWithoutUnit()
				+ ";" + "|Allowed:"
				+ this.getAllowedDischargePower().asStringWithoutUnit() + ";"
				+ this.getAllowedChargePower().asString();
	}

	@Override
	public void handleEvent(Event event) {
		if (!this.isEnabled()) {
			return;
		}
		switch (event.getTopic()) {
		case EdgeEventConstants.TOPIC_CYCLE_AFTER_PROCESS_IMAGE:
			this.applyPowerLimitOnPowerDecreaseCausedByOvertemperatureError();
			this.calculateEnergy();
			break;
		case EdgeEventConstants.TOPIC_CYCLE_BEFORE_CONTROLLERS:
			this.defineWorkState();
			break;
		}
	}

	private LocalDateTime lastDefineWorkState = null;

	private void defineWorkState() {
		/*
		 * Set ESS in running mode
		 */
		// TODO this should be smarter: set in energy saving mode if there was no output
		// power for a while and we don't need emergency power.
		var now = LocalDateTime.now();
		if (this.lastDefineWorkState == null || now.minusMinutes(1).isAfter(this.lastDefineWorkState)) {
			this.lastDefineWorkState = now;
			EnumWriteChannel setWorkStateChannel = this.channel(DeyeSunHybrid.ChannelId.SET_WORK_STATE);
			try {
				setWorkStateChannel.setNextWriteValue(SetWorkState.START);
			} catch (OpenemsNamedException e) {
				this.logError(this.log, "Unable to start: " + e.getMessage());
			}
		}
	}

	@Override
	public Power getPower() {
		return this.power;
	}

	@Override
	public boolean isManaged() {
		return !this.config.readOnlyMode();
	}

	@Override
	public int getPowerPrecision() {
		return 100; // the modbus field for SetActivePower has the unit 0.1 kW
	}

	@Override
	public Constraint[] getStaticConstraints() throws OpenemsNamedException {
		//return Power.NO_CONSTRAINTS;
		// Read-Only-Mode
		//if (this.config.readOnlyMode()) {
		//	return new Constraint[] { //
		//			this.createPowerConstraint("Read-Only-Mode", Phase.ALL, Pwr.ACTIVE, Relationship.EQUALS, 0), //
		//			this.createPowerConstraint("Read-Only-Mode", Phase.ALL, Pwr.REACTIVE, Relationship.EQUALS, 0) //
		//	};
		//}

		// Reactive Power constraints
		return new Constraint[] { //
				this.createPowerConstraint("Deye Min Active Power", Phase.ALL, Pwr.ACTIVE,
						Relationship.GREATER_OR_EQUALS, -10000),
				this.createPowerConstraint("Deye Max Active Power", Phase.ALL, Pwr.ACTIVE,
						Relationship.LESS_OR_EQUALS, 10000),
				this.createPowerConstraint("Deye Min Reactive Power", Phase.ALL, Pwr.REACTIVE,
						Relationship.GREATER_OR_EQUALS, MIN_REACTIVE_POWER), //
				this.createPowerConstraint("Deye Max Reactive Power", Phase.ALL, Pwr.REACTIVE,
						Relationship.LESS_OR_EQUALS, MAX_REACTIVE_POWER) };
	}

	@Override
	public ModbusSlaveTable getModbusSlaveTable(AccessMode accessMode) {
		return new ModbusSlaveTable(//
				OpenemsComponent.getModbusSlaveNatureTable(accessMode), //
				SymmetricEss.getModbusSlaveNatureTable(accessMode), //
				ManagedSymmetricEss.getModbusSlaveNatureTable(accessMode) //
		);
	}

	@Override
	protected void logInfo(Logger log, String message) {
		super.logInfo(log, message);
	}

	private void applyPowerLimitOnPowerDecreaseCausedByOvertemperatureError() {
		if (this.config.powerLimitOnPowerDecreaseCausedByOvertemperatureChannel() != 0) {
			StateChannel powerDecreaseCausedByOvertemperatureChannel = this
					.channel(DeyeSunHybrid.ChannelId.POWER_DECREASE_CAUSED_BY_OVERTEMPERATURE);
			if (powerDecreaseCausedByOvertemperatureChannel.value().orElse(false)) {
				/*
				 * Apply limit on ESS charge/discharge power
				 */
				try {
					this.power.addConstraintAndValidate(
							this.createPowerConstraint("Limit On PowerDecreaseCausedByOvertemperature Error", Phase.ALL,
									Pwr.ACTIVE, Relationship.GREATER_OR_EQUALS,
									this.config.powerLimitOnPowerDecreaseCausedByOvertemperatureChannel() * -1));
					this.power.addConstraintAndValidate(
							this.createPowerConstraint("Limit On PowerDecreaseCausedByOvertemperature Error", Phase.ALL,
									Pwr.ACTIVE, Relationship.LESS_OR_EQUALS,
									this.config.powerLimitOnPowerDecreaseCausedByOvertemperatureChannel()));
				} catch (OpenemsException e) {
					this.logError(this.log, e.getMessage());
				}

			}
		}
	}

	@Override
	public Timedata getTimedata() {
		return this.timedata;
	}

	private void calculateEnergy() {
		/*
		 * Calculate AC Energy
		 */
		var acActivePower = this.getActivePowerChannel().getNextValue().get();
		if (acActivePower == null) {
			// Not available
			this.calculateAcChargeEnergy.update(null);
			this.calculateAcDischargeEnergy.update(null);
		} else if (acActivePower > 0) {
			// Discharge
			this.calculateAcChargeEnergy.update(0);
			this.calculateAcDischargeEnergy.update(acActivePower);
		} else {
			// Charge
			this.calculateAcChargeEnergy.update(acActivePower * -1);
			this.calculateAcDischargeEnergy.update(0);
		}

		/*
		 * Calculate DC Power and Energy
		 */
		var dcDischargePower = acActivePower;

		if (dcDischargePower == null) {
			// Not available
			this.calculateDcChargeEnergy.update(null);
			this.calculateDcDischargeEnergy.update(null);
		} else if (dcDischargePower > 0) {
			// Discharge
			this.calculateDcChargeEnergy.update(0);
			this.calculateDcDischargeEnergy.update(dcDischargePower);
		} else {
			// Charge
			this.calculateDcChargeEnergy.update(dcDischargePower * -1);
			this.calculateDcDischargeEnergy.update(0);
		}
	}

}
