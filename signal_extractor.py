# signal_extractor_professional.py
import json
import os
import glob
from datetime import datetime
from typing import Dict, List, Optional

class ProfessionalSignalExtractor:
    def __init__(self, data_directory: str = "docs"):
        self.data_dir = data_directory
        self.supported_exchanges = ["binance", "gate", "mexc", "coinex", "htx", "bitmart", "lbank"]
        
    def extract_complete_dataset(self) -> Dict:
        """Extract complete Tier 1 + Tier 2 dataset from existing bot files"""
        
        print("🔍 EXTRACTING SIGNALS FROM EXISTING BOT DATA...")
        
        # Extract Tier 1 signals (all original opportunities from tokens2e files)
        tier1_signals = self._extract_tier1_from_tokens2e_files()
        
        # Extract Tier 2 signals (validated opportunities + failures)
        tier2_results = self._extract_tier2_from_bot2_files()
        
        # Calculate statistics
        total_detected = len(tier1_signals)
        total_passed = len(tier2_results['passed'])
        total_failed = len(tier2_results['failed'])
        success_rate = (total_passed / total_detected * 100) if total_detected > 0 else 0
        
        complete_dataset = {
            'extraction_timestamp': datetime.utcnow().isoformat(),
            'data_source': 'Live bot execution results',
            'tier1_product': {
                'name': 'Complete Market Scanner',
                'price_monthly': 97,
                'description': 'All detected arbitrage opportunities across all exchanges',
                'signals': tier1_signals,
                'total_opportunities': total_detected,
                'exchanges_covered': self._get_active_exchanges(tier1_signals),
                'profit_range': self._calculate_profit_range(tier1_signals),
                'average_profit': self._calculate_average_profit(tier1_signals)
            },
            'tier2_product': {
                'name': 'Execution-Ready Signals',
                'price_monthly': 297,
                'description': 'Validated opportunities with real-time execution analysis',
                'signals_passed': tier2_results['passed'],
                'signals_failed': tier2_results['failed'],
                'total_detected': total_detected,
                'total_passed': total_passed,
                'total_failed': total_failed,
                'success_rate_percent': round(success_rate, 1),
                'validation_summary': tier2_results['validation_summary'],
                'failure_analysis': tier2_results['failure_breakdown']
            },
            'value_proposition': {
                'tier1_value': f"Access to {total_detected} daily opportunities across {len(self._get_active_exchanges(tier1_signals))} exchanges",
                'tier2_value': f"Execute {total_passed} validated opportunities, avoid {total_failed} potential losses",
                'risk_reduction_percent': round((total_failed / total_detected * 100), 1) if total_detected > 0 else 0,
                'upgrade_justification': f"Tier 2 prevents {total_failed} potential failed trades worth protection"
            },
            'system_performance': {
                'detection_accuracy': f"{total_detected} opportunities found",
                'validation_accuracy': f"{success_rate:.1f}% pass validation",
                'risk_filtering': f"{total_failed} risky opportunities filtered out",
                'data_freshness': 'Real-time from live trading engine'
            }
        }
        
        print(f"✅ EXTRACTION COMPLETE:")
        print(f"   📊 Tier 1: {total_detected} total opportunities")
        print(f"   ✅ Tier 2: {total_passed} validated, {total_failed} filtered")
        print(f"   📈 Success rate: {success_rate:.1f}%")
        
        return complete_dataset
    
    def _extract_tier1_from_bot1_files(self) -> List[Dict]:
        """Extract all planned opportunities from cex_bot1.json files"""
        
        tier1_signals = []
        
        for exchange in self.supported_exchanges:
            bot1_file = os.path.join(self.data_dir, f"{exchange}_bot1.json")
            
            if os.path.exists(bot1_file):
                try:
                    with open(bot1_file, 'r') as f:
                        planned_cycles = json.load(f)
                    
                    print(f"📁 {exchange}_bot1.json: {len(planned_cycles)} planned cycles")
                    
                    for cycle in planned_cycles:
                        signal = self._format_tier1_signal(cycle, exchange)
                        tier1_signals.append(signal)
                        
                except Exception as e:
                    print(f"❌ Error reading {bot1_file}: {e}")
            else:
                print(f"⚠️ Missing: {bot1_file}")
        
        print(f"📊 TIER 1 TOTAL: {len(tier1_signals)} opportunities extracted")
        return tier1_signals
    
    def _extract_tier2_from_bot2_files(self) -> Dict:
        """Extract validated results from cex_bot2.json files"""
        
        passed_signals = []
        failed_signals = []
        validation_summary = {}
        failure_reasons = {}
        
        for exchange in self.supported_exchanges:
            bot2_file = os.path.join(self.data_dir, f"{exchange}_bot2.json")
            
            if os.path.exists(bot2_file):
                try:
                    with open(bot2_file, 'r') as f:
                        bot2_data = json.load(f)
                        validated_cycles = bot2_data.get("simulated_cycles", [])
                    
                    print(f"📁 {exchange}_bot2.json: {len(validated_cycles)} validated cycles")
                    
                    exchange_passed = 0
                    exchange_failed = 0



                    for cycle in validated_cycles:
                        executable_status = cycle.get('executable', False)
                        simulation_result = cycle.get('simulation_result', 'UNKNOWN')
                        print(f"DEBUG: Cycle {cycle.get('cycle_id')}: executable={executable_status}, result={simulation_result}")
    
                        if self._is_cycle_executable(cycle):
                            print(f"  -> PASSED VALIDATION")
                            signal = self._format_tier2_passed_signal(cycle, exchange)
                            passed_signals.append(signal)
                            exchange_passed += 1
                        else:
                            print(f"  -> FAILED VALIDATION")
                            failed_signal = self._format_tier2_failed_signal(cycle, exchange)
                            failed_signals.append(failed_signal)
                            exchange_failed += 1






                            
                            # Track failure reasons
                            reason = self._extract_failure_reason(cycle)
                            failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
                    
                    validation_summary[exchange] = {
                        'total_validated': len(validated_cycles),
                        'passed': exchange_passed,
                        'failed': exchange_failed,
                        'success_rate': round((exchange_passed / len(validated_cycles) * 100), 1) if validated_cycles else 0
                    }
                    
                except Exception as e:
                    print(f"❌ Error reading {bot2_file}: {e}")
            else:
                print(f"⚠️ Missing: {bot2_file}")
        
        print(f"✅ TIER 2: {len(passed_signals)} passed, {len(failed_signals)} failed")
        
        return {
            'passed': passed_signals,
            'failed': failed_signals,
            'validation_summary': validation_summary,
            'failure_breakdown': failure_reasons
        }


    def _extract_tier1_from_tokens2e_files(self) -> List[Dict]:
        """Extract all original opportunities from cex_tokens2e.json files"""
        
        tier1_signals = []
        
        for exchange in self.supported_exchanges:
            tokens2e_file = os.path.join(self.data_dir, f"{exchange}_tokens2e.json")
            
            if os.path.exists(tokens2e_file):
                try:
                    with open(tokens2e_file, 'r') as f:
                        tokens_data = json.load(f)
                    
                    print(f"📁 {exchange}_tokens2e.json: {len(tokens_data)} token opportunities")
                    
                    for token_data in tokens_data:
                        # Extract arbitrage routes from each token
                        arbitrage_routes = token_data.get('arbitrageRoutes', [])
                        for route in arbitrage_routes:
                            if route.get('status') == 'PROFITABLE':
                                signal = self._format_tier1_signal_from_tokens2e(token_data, route, exchange)
                                tier1_signals.append(signal)
                        
                except Exception as e:
                    print(f"❌ Error reading {tokens2e_file}: {e}")
            else:
                print(f"⚠️ Missing: {tokens2e_file}")
        
        print(f"📊 TIER 1 TOTAL: {len(tier1_signals)} opportunities extracted")
        return tier1_signals
    
    def _format_tier1_signal_from_tokens2e(self, token_data: Dict, route: Dict, exchange: str) -> Dict:
        """Format token opportunity from tokens2e file into Tier 1 signal"""
        
        symbol = token_data.get('symbol', 'UNKNOWN')
        route_id = route.get('routeId', 'unknown')
        
        # Extract profitability data
        profitability = route.get('profitability', {})
        profit_percent = profitability.get('profitPercent', 0)
        profit_amount = profitability.get('profit', 0)
        
        # Extract trading route
        from_exchange = exchange.upper()
        to_exchange = route.get('toCex', 'UNKNOWN').upper()
        network = route.get('network', 'UNKNOWN')
        
        return {
            'signal_id': f"T1_{from_exchange}_{symbol}_{route_id}_{int(datetime.utcnow().timestamp())}",
            'timestamp': datetime.utcnow().isoformat(),
            'tier': 1,
            'source_exchange': from_exchange,
            'route_id': route_id,
            'token_symbol': symbol,
            'token_pair': f"{symbol}/USDT",
            'trading_route': f"{from_exchange} → {to_exchange}",
            'total_profit_percent': round(profit_percent, 3),
            'required_capital_usdt': profitability.get('requiredCapital', 500),
            'expected_profit_usdt': round(profit_amount, 2),
            'execution_time_minutes': route.get('estimatedDuration', 15),
            'network_primary': network,
            'network_secondary': None,  # Single hop from tokens2e
            'total_score': route.get('score', 0),
            'route_details': {
                'from_exchange': from_exchange,
                'to_exchange': to_exchange,
                'network': network,
                'status': route.get('status', 'UNKNOWN'),
                'confidence': route.get('confidence', 0.8)
            },
            'raw_data': route  # Include full route data for debugging
        }

    def _format_tier1_signal_from_bot2(self, cycle: Dict, exchange: str) -> Dict:
        """Format bot2 cycle into signal format for Tier 2 processing"""
        
        symbol = cycle.get('symbol', 'UNKNOWN')
        cycle_id = cycle.get('cycle_id', 'unknown')
        total_profit = cycle.get('total_profit_percent', 0)
        
        hop1 = cycle.get('hop1', {})
        hop2 = cycle.get('hop2', {})
        
        from_exchange = hop1.get('from_exchange', exchange.upper())
        intermediate_exchange = hop1.get('to_exchange', 'UNKNOWN')
        final_exchange = hop2.get('to_exchange', intermediate_exchange) if hop2 else intermediate_exchange
        
        trading_route = f"{from_exchange} → {intermediate_exchange}"
        if hop2 and final_exchange != intermediate_exchange:
            trading_route += f" → {final_exchange}"
        
        required_capital = hop1.get('required_capital', 100)
        profit_amount = (total_profit / 100) * required_capital
        
        return {
            'signal_id': f"T2_{exchange.upper()}_{cycle_id}_{int(datetime.utcnow().timestamp())}",
            'timestamp': datetime.utcnow().isoformat(),
            'tier': 2,
            'source_exchange': exchange.upper(),
            'cycle_id': cycle_id,
            'token_symbol': symbol,
            'token_pair': f"{symbol}/USDT",
            'trading_route': trading_route,
            'total_profit_percent': round(total_profit, 3),
            'required_capital_usdt': required_capital,
            'expected_profit_usdt': round(profit_amount, 2),
            'execution_time_minutes': cycle.get('estimated_execution_time', 15),
            'network_primary': hop1.get('network', 'UNKNOWN'),
            'raw_data': cycle
        }


    
    def _format_tier1_signal_from_tokens2e(self, token_data: Dict, route: Dict, exchange: str) -> Dict:
        """Format token opportunity from tokens2e file into Tier 1 signal"""
        
        symbol = token_data.get('symbol', 'UNKNOWN')
        route_id = route.get('routeId', 'unknown')
        
        # Extract profitability data
        profitability = route.get('profitability', {})
        profit_percent = profitability.get('profitPercent', 0)
        profit_amount = profitability.get('profit', 0)
        
        # Extract trading route
        from_exchange = exchange.upper()
        to_exchange = route.get('toCex', 'UNKNOWN').upper()
        network = route.get('network', 'UNKNOWN')
        
        return {
            'signal_id': f"T1_{from_exchange}_{symbol}_{route_id}_{int(datetime.utcnow().timestamp())}",
            'timestamp': datetime.utcnow().isoformat(),
            'tier': 1,
            'source_exchange': from_exchange,
            'route_id': route_id,
            'token_symbol': symbol,
            'token_pair': f"{symbol}/USDT",
            'trading_route': f"{from_exchange} → {to_exchange}",
            'total_profit_percent': round(profit_percent, 3),
            'required_capital_usdt': profitability.get('requiredCapital', 500),
            'expected_profit_usdt': round(profit_amount, 2),
            'execution_time_minutes': route.get('estimatedDuration', 15),
            'network_primary': network,
            'network_secondary': None,  # Single hop from tokens2e
            'total_score': route.get('score', 0),
            'route_details': {
                'from_exchange': from_exchange,
                'to_exchange': to_exchange,
                'network': network,
                'status': route.get('status', 'UNKNOWN'),
                'confidence': route.get('confidence', 0.8)
            },
            'raw_data': route  # Include full route data for debugging
        }
    
    def _format_tier2_passed_signal(self, cycle: Dict, exchange: str) -> Dict:
        """Format validated executable cycle into Tier 2 passed signal"""
        
        base_signal = self._format_tier1_signal_from_bot2(cycle, exchange)
        
        # Add Tier 2 specific validation data
        base_signal.update({
            'signal_id': base_signal['signal_id'].replace('T1_', 'T2_PASS_'),
            'tier': 2,
            'validation_status': 'PASSED',
            'executable': cycle.get('executable', True),
            'execution_confidence': cycle.get('execution_confidence', 0.95),
            'simulation_result': cycle.get('simulation_result', 'PASSED'),
            'validation_timestamp': cycle.get('simulation_timestamp', datetime.utcnow().isoformat()),
            'risk_assessment': {
                'liquidity_sufficient': True,
                'network_healthy': True,
                'price_stable': True,
                'execution_ready': True
            }
        })
        
        return base_signal
    
    def _format_tier2_failed_signal(self, cycle: Dict, exchange: str) -> Dict:
        """Format failed validation cycle into Tier 2 failed signal with reason"""
        
        base_signal = self._format_tier1_signal_from_bot2(cycle, exchange)
        
        # Add failure analysis
        failure_reason = self._extract_failure_reason(cycle)
        failure_category = self._categorize_failure(cycle)
        
        base_signal.update({
            'signal_id': base_signal['signal_id'].replace('T1_', 'T2_FAIL_'),
            'tier': 2,
            'validation_status': 'FAILED',
            'executable': False,
            'execution_confidence': cycle.get('execution_confidence', 0.0),
            'simulation_result': cycle.get('simulation_result', 'FAILED'),
            'validation_timestamp': cycle.get('simulation_timestamp', datetime.utcnow().isoformat()),
            'failure_reason': failure_reason,
            'failure_category': failure_category,
            'risk_assessment': {
                'liquidity_sufficient': 'liquidity' not in failure_reason.lower(),
                'network_healthy': 'network' not in failure_reason.lower(),
                'price_stable': 'price' not in failure_reason.lower(),
                'execution_ready': False
            }
        })
        
        return base_signal


    def _is_cycle_executable(self, cycle: Dict) -> bool:
        """Determine if cycle passed validation"""
        return (
            cycle.get('executable', False) and 
            cycle.get('simulation_result', '').upper() == 'SUCCESS'
        )


    
    def _extract_failure_reason(self, cycle: Dict) -> str:
        """Extract human-readable failure reason"""
        
        failure_reason = cycle.get('failure_reason', 'Unknown validation failure')
        
        # Clean up technical failure reasons for customer display
        if 'profit below threshold' in failure_reason.lower():
            return "Profit margin too low after real-time price check"
        elif 'liquidity' in failure_reason.lower():
            return "Insufficient liquidity on exchange orderbook"
        elif 'price' in failure_reason.lower():
            return "Price moved unfavorably since initial detection"
        elif 'network' in failure_reason.lower():
            return "Network congestion or high transfer fees"
        elif 'timeout' in failure_reason.lower():
            return "Exchange API response too slow for execution"
        else:
            return failure_reason
    
    def _categorize_failure(self, cycle: Dict) -> str:
        """Categorize failure type for statistics"""
        
        failure_category = cycle.get('failure_category', 'UNKNOWN')
        
        category_map = {
            'PRICE_MOVEMENT': 'Market Price Change',
            'LIQUIDITY_INSUFFICIENT': 'Low Liquidity',
            'NETWORK_CONGESTION': 'Network Issues',
            'API_TIMEOUT': 'Exchange Problems',
            'PROFIT_TOO_LOW': 'Profit Threshold',
            'UNKNOWN': 'Technical Issues'
        }
        
        return category_map.get(failure_category, failure_category)
    
    def _get_active_exchanges(self, signals: List[Dict]) -> List[str]:
        """Get list of exchanges with active signals"""
        exchanges = set()
        for signal in signals:
            exchanges.add(signal.get('source_exchange', 'UNKNOWN'))
        return sorted(list(exchanges))
    
    def _calculate_profit_range(self, signals: List[Dict]) -> Dict:
        """Calculate profit range statistics"""
        if not signals:
            return {'min': 0, 'max': 0}
        
        profits = [signal.get('total_profit_percent', 0) for signal in signals]
        return {
            'min_percent': round(min(profits), 2),
            'max_percent': round(max(profits), 2)
        }
    
    def _calculate_average_profit(self, signals: List[Dict]) -> float:
        """Calculate average profit percentage"""
        if not signals:
            return 0.0
        
        profits = [signal.get('total_profit_percent', 0) for signal in signals]
        return round(sum(profits) / len(profits), 2)

# Test the extractor
if __name__ == "__main__":
    extractor = ProfessionalSignalExtractor()
    
    print("🚀 PROFESSIONAL SIGNAL EXTRACTOR - LIVE DATA EXTRACTION")
    print("=" * 70)
    
    # Extract complete dataset
    complete_data = extractor.extract_complete_dataset()
    
    # Display summary
    tier1 = complete_data['tier1_product']
    tier2 = complete_data['tier2_product']
    value_prop = complete_data['value_proposition']






    print("\n" + "━" * 70)
    print("📊 TIER 1 SIGNALS - Complete Market Scanner ($97/month)")
    print("━" * 70)
    for i, signal in enumerate(tier1['signals'][:10], 1):
        print(f"\nSIGNAL #{i}: {signal['token_symbol']} - {signal['total_profit_percent']}% PROFIT")
        print(f"Route: {signal['trading_route']}")
        print(f"Capital Required: ${signal['required_capital_usdt']}")
        print(f"Expected Profit: ${signal['expected_profit_usdt']}")
        print(f"Network: {signal['network_primary']}")
    print(f"\n... {len(tier1['signals']) - 10} more signals available in full product")
    print("\n" + "━" * 70)  
    print("✅ TIER 2 SIGNALS - Execution-Ready ($297/month)")
    print("━" * 70)
    print(f"\n🎯 EXECUTABLE NOW ({tier2['total_passed']} signals):")
    for i, signal in enumerate(tier2['signals_passed'][:5], 1):
        confidence = int(signal.get('execution_confidence', 0) * 100)
        print(f"\nSIGNAL #{i}: {signal['token_symbol']} - {signal['total_profit_percent']}% ✅ VALIDATED")
        print(f"Route: {signal['trading_route']}")
        print(f"Execution Confidence: {confidence}%")
        print(f"Status: EXECUTE IMMEDIATELY")
    print(f"\n❌ FILTERED OUT ({tier2['total_failed']} signals):")
    for i, signal in enumerate(tier2['signals_failed'][:3], 1):
        print(f"\nSIGNAL #{i}: {signal['token_symbol']} - {signal['total_profit_percent']}% ❌ FILTERED")
        print(f"Reason: {signal['failure_reason']}")
        print(f"Risk Avoided: Potential execution failure")
    # Save files
    with open("tier1_customer_signals.json", "w") as f:
        json.dump(tier1, f, indent=2)
    with open("tier2_customer_signals.json", "w") as f:
        json.dump(tier2, f, indent=2)
    
    print(f"\n💾 Customer data saved to JSON files")
    print("━" * 70)