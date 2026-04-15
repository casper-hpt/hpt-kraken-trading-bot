from hpt_llm.tools.supply_chain_theory import generate_supply_chain_theory


def test_generate_supply_chain_theory():
    result = generate_supply_chain_theory("72h")
    print(result)


if __name__ == "__main__":
    test_generate_supply_chain_theory()
