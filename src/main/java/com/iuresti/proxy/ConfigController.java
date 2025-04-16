package com.iuresti.proxy;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
public class ConfigController {

    private final Map<Integer, TcpForwarder> activeRules = new HashMap<>();

    @PostMapping("forward")
    public ResponseEntity<Void> startListening(@RequestBody ForwardRule forwardRule) {

        if (!activeRules.containsKey(forwardRule.getListenPort())) {
            TcpForwarder tcpForwarder = new TcpForwarder(forwardRule);
            activeRules.put(forwardRule.getListenPort(), tcpForwarder);

            tcpForwarder.start();
        } else {
            return ResponseEntity.badRequest().build();
        }

        return ResponseEntity.ok().build();
    }

    @GetMapping("forward")
    public ResponseEntity<Map<Integer, TcpForwarder>> activeListeners() {

        return ResponseEntity.ok(activeRules);
    }

    @DeleteMapping("forward/{port}")
    public ResponseEntity<Void> stopListening(@PathVariable int port) {

        if (activeRules.containsKey(port)) {
            activeRules.get(port).stop();
            activeRules.remove(port);
        } else {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.noContent().build();
    }

    @PostMapping("forward/block-responses/{port}")
    public ResponseEntity<Void> blockResponses(@PathVariable int port) {
        if (activeRules.containsKey(port)) {
            activeRules.get(port).blockInput();
        } else {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok().build();
    }

    @PostMapping("forward/unblock-responses/{port}")
    public ResponseEntity<Void> unblockResponses(@PathVariable int port) {
        if (activeRules.containsKey(port)) {
            activeRules.get(port).unblockInput();
        } else {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok().build();
    }

    @PostMapping("forward/block-requests/{port}")
    public ResponseEntity<Void> blockRequests(@PathVariable int port) {
        if (activeRules.containsKey(port)) {
            activeRules.get(port).blockOutput();
        } else {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok().build();
    }

    @PostMapping("forward/unblock-requests/{port}")
    public ResponseEntity<Void> unblockRequests(@PathVariable int port) {
        if (activeRules.containsKey(port)) {
            activeRules.get(port).unblockOutput();
        } else {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok().build();
    }

}
