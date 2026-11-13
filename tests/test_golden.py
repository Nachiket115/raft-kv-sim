import hashlib

import pytest

from raftlite.scenarios import fuzz, run

GOLDEN = {
    ("leader_partitioned", 0): "677bbed4483a03da7490297286b73ca37007a28860abc33ba49eb6b2777f9f10",
    ("leader_partitioned", 1): "3722ab6a8daeb62efb8a34e3861842cfb2dc7dbdfe56935c056e1c096764b005",
    ("leader_partitioned", 2): "b27bba3507e72ebc98035aba7b8b6a941f312d350307b1dc30d77e16e500c394",
    ("minority_partition", 0): "ac908ee7e681e38fc0a496fc43236d29c2330d787c7e1a3d080c9d0569aafb26",
    ("minority_partition", 1): "69726111027691380b14b2d51761c2ed5e512de433d2d7385a46e5cd90150bf2",
    ("minority_partition", 2): "b08490f12dac40c71b2cd5993b46d58aa4a59c33cbbb342a341312956b766ea7",
    ("follower_catches_up", 0): "6f74a5a6da09a41a95a7d7cc5f01a67adeabeba806469ce41c204da89a2aced9",
    ("follower_catches_up", 1): "944b7937f2acb2ecd0b151412289c078c6136d65ff3cc80d302d9231a9a50982",
    ("follower_catches_up", 2): "46e7e8bd7e13dfb17cf4f5c3b0ede5613473bc5108ccb976249a9b53f0471749",
    ("split_vote", 0): "4b9a0fae2901334a2dad7e6f852804c5865689d63dac2e1a7e519b19b7df47f5",
    ("split_vote", 1): "59b4947df9c999bd675f80087f038e67fb2da7a4977f5986eaab276d0174202c",
    ("split_vote", 2): "556a3eb60ac37ba4dfecc2b295f8e5f27f9567d37b80f87805bc82f5fa29ac76",
    ("stale_leader_steps_down", 0): "1fd4dbf5543c54d22dccff7c010f60eee98868a54db2ad4f456ee0b80e261083",
    ("stale_leader_steps_down", 1): "37f796ce9e8f8ef31859c81e37aea7264f6647abadb1bc89a46eb869da5a73d9",
    ("stale_leader_steps_down", 2): "87772013a93c08b3dd1b557ca6c5570abf42c58ca936b62ce9ce8e211d8f87b6",
    ("liveness", 0): "636f9b0b27ded66c456f3dface26356ec508a15555d3b1ce71a4274109cd3d21",
    ("liveness", 1): "eac547867b4d2cb00561c46649252c9eb6d8d82af813cfb59a36615fe872c2b2",
    ("liveness", 2): "cd5b2a35eb393d15fca8fc7742553d88a0cfedfaf0f4797557e50aef30f8d71e",
    ("figure8", 0): "2e0e81e141d75da24ddcabc17ba889b85d83c3e87522b3bd0ea80d244fc56479",
    ("figure8", 1): "8b3f2e99dda8b6faf0954974187aca777af8d2ec6cc722dd785a483947e9a750",
    ("figure8", 2): "6ee9e2fd46294cd9486f86dde615c04fb786c19f86ae5ba837b92f4c76a356c2",
    ("fuzz", 0): "926dd877b5bbc8ff44bd000fab0b8c1054a1ccd52557f19490f91f01b8631994",
    ("fuzz", 1): "ea23b26a16f9b27991b593832466fb871917b8cc9ce3c96554c4804670c0fea9",
    ("fuzz", 2): "f03de6b3b3d939aa9ffca509e8b0fecf35682b67eb997577d79f1a4549df0e31",
}


@pytest.mark.parametrize("name, seed", GOLDEN, ids=lambda value: str(value))
def test_trace_is_byte_identical_to_the_pinned_one(name, seed):
    sim = fuzz(seed) if name == "fuzz" else run(name, seed)
    digest = hashlib.sha256(sim.trace_text().encode()).hexdigest()
    assert digest == GOLDEN[name, seed]
